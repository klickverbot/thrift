/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
module thrift.async.libevent;

import core.time : Duration, dur;
import core.exception : onOutOfMemoryError;
import core.memory : GC;
import core.thread : Fiber, Thread;
import core.stdc.stdlib : free, malloc;
import core.sys.posix.sys.time : timeval; // TODO: Windows.
import std.conv : text, to;
import std.array : empty, front, popFront;
import std.socket;
import std.stdio : stderr;
import thrift.base;
import thrift.async.base;
import thrift.c.event.event;
import thrift.util.socket;

/**
 * A TAsyncManager implementation based on libevent.
 */
// TODO: Provide some means to shut down the socket manager worker thread.
class TLibeventAsyncManager : TAsyncSocketManager {
  this() {
    eventBase_ = event_base_new();

    // Set up the socket pair for transferring work to the event loop.
    auto pair = socketPair();
    workSendSocket_ = pair[0];
    workReceiveSocket_ = pair[1];
    workReceiveSocket_.blocking = false;

    // Register an event for receiving new work.
    workReceiveEvent_ = event_new(eventBase_, workReceiveSocket_.handle,
      EV_READ | EV_PERSIST, &workReceiveCallback, cast(void*)this);
    event_add(workReceiveEvent_, null);
  }

  ~this() {
    event_free(workReceiveEvent_);
    event_base_free(eventBase_);
    eventBase_ = null;
  }

  override void execute(TAsyncWorkItem workItem) {
    if (!workerThread_) {
      workerThread_ = new Thread({ event_base_loop(eventBase_, 0); });
      // TODO: Once a mechanism for controlled shutting down of the worker
      // thread has been added, no longer daemonize it to avoid crashes during
      // shutdown. Also, try restarting the worker thread if it crashed?
      workerThread_.isDaemon = true;
      workerThread_.start();
    }

    // We should be able to send the work item as a whole – we currently
    // assume to be able to receive it at once as well. If this proves to be
    // unstable (e.g. send could possibly return early if the receiving buffer
    // is full and the blocking call gets interrupted by a signal), it could
    // be changed to a more sophisticated scheme.

    // Make sure the delegate context doesn't get GCd while the work item is
    // on the wire. TODO: The following is just a stab in the dark, I am not
    // sure if it actually works as expected.
    GC.addRoot(workItem.work.ptr);

    auto result = workSendSocket_.send((&workItem)[0 .. 1]);
    enum size = workItem.sizeof;
    enforce(result == size, new TException(text("Sending work item failed (",
      result, " bytes instead of ", size, " trasmitted).")));
  }

  override void addOneshotListener(Socket socket, TAsyncEventType eventType,
     SocketEventListener listener
  ) {
    addOneshotListenerImpl(socket, eventType, null, listener);
  }

  override void addOneshotListener(Socket socket, TAsyncEventType eventType,
    Duration timeout, SocketEventListener listener
  ) {
    if (timeout <= dur!"hnsecs"(0)) {
      addOneshotListenerImpl(socket, eventType, null, listener);
    } else {
      // This is not really documented well, but libevent does not require to
      // keep the timeval around after the event was added.
      timeval tv = { tv_sec: cast(int)timeout.total!"seconds"(),
        tv_usec: timeout.fracSec.usecs };
      addOneshotListenerImpl(socket, eventType, &tv, listener);
    }
  }

private:
  void addOneshotListenerImpl(Socket socket, TAsyncEventType eventType,
     const(timeval)* timeout, SocketEventListener listener
  ) {
    // Create a copy of the listener delegate on the C heap.
    auto listenerMem = malloc(listener.sizeof);
    if (!listenerMem) onOutOfMemoryError();
    (cast(SocketEventListener*)listenerMem)[0 .. 1] = listener;
    GC.addRange(listenerMem, listener.sizeof);

    // Add a libevent oneshot event for it.
    auto result = event_base_once(eventBase_, socket.handle,
      libeventEventType(eventType), &invokeListenerCallback,
      listenerMem, timeout);

    // Assuming that we didn't get our arguments wrong above, the only other
    // situation in which event_base_once can fail is when it can't allocate
    // memory.
    if (result != 0) onOutOfMemoryError();
  }

  /**
   * Receives a work item from the work receive socket and adds it to the
   * queue. Called from the worker thread.
   */
  void receiveWork() {
    // Read as many new work items off the socket as possible (at least one
    // should be available, as we got notified by libevent).
    TAsyncWorkItem workItem;
    ptrdiff_t bytesRead;
    while (true) {
      bytesRead = workReceiveSocket_.receive(
        cast(ubyte[])((&workItem)[0 .. 1]));

      if (bytesRead < 0) {
        auto errno = getSocketErrno();
        if (errno != WOULD_BLOCK_ERRNO) {
          stderr.writefln("TLibevent…SocketManger.receiveWork(): read " ~
            "failed, some work item will never be executed: %s",
            socketErrnoString(errno));
        }
      }
      if (bytesRead != workItem.sizeof) break;

      // Everything went fine, we got a brand new work item.

      // Now that the work item is back in the D world, we don't need the
      // extra GC root for the context pointer anymore (see execute()).
      GC.removeRoot(workItem.work.ptr);

      // Add the work item to the queue and execute it.
      auto queue = workItem.transport in workQueues_;
      if (queue is null || (*queue).empty) {
        // If the queue is empty, add the new work item to the queue as well,
        // but immediately start executing it.
        workQueues_[workItem.transport] = [workItem];
        executeWork(workItem);
      } else {
        (*queue) ~= workItem;
      }
    }

    // If the last read was successful, but didn't read enough bytes, we got
    // a problem.
    if (bytesRead > 0) {
      stderr.writefln("TLibevent…SocketManger.receiveWork(): Unexpected " ~
        "partial read (%s bytes instead of %s), some work item will never" ~
        "be executed.", bytesRead, workItem.sizeof);
    }
  }

  /**
   * Executes a work item and all folliwing items waiting in the same queue.
   */
  void executeWork(TAsyncWorkItem workItem) {
    (new Fiber({
      auto item = workItem;
      while (true) {
        // Execute the actual work. It will possibly add listeners to the
        // event loop and yield away if it has to wait for blocking operations.
        item.work();

        // Remove the item from the work queue.
        // Note: Due to the value semantics of array slices, we have to
        // re-lookup this on every iteration. This could be solved, but I'd
        // rather replace this directly with a queue type once one becomes
        // available in Phobos.
        auto queue = workQueues_[item.transport];
        assert(queue.front == item);
        queue.popFront();
        workQueues_[workItem.transport] = queue;

        if (queue.empty) break;

        // If the queue is not empty, execute the next waiting item.
        item = queue.front;
      }
    })).call();
  }

  static extern(C) void workReceiveCallback(evutil_socket_t, short,
    void *managerThis
  ) {
    (cast(TLibeventAsyncManager)managerThis).receiveWork();
  }

  static extern(C) void invokeListenerCallback(evutil_socket_t, short flags,
    void *arg
  ) {
    auto reason = (flags & EV_TIMEOUT) ? TAsyncEventReason.TIMED_OUT :
      TAsyncEventReason.NORMAL;
    (*(cast(SocketEventListener*)arg))(reason);
    GC.removeRange(arg);
    clear(arg);
    free(arg);
  }

  static short libeventEventType(TAsyncEventType type) {
    final switch (type) {
      case TAsyncEventType.READ:
        return EV_READ;
      case TAsyncEventType.WRITE:
        return EV_WRITE;
    }
  }

  Thread workerThread_;

  event_base* eventBase_;

  /// The socket used for receiving new work items in the event loop. Paired
  /// with workSendSocket_.
  Socket workReceiveSocket_;
  event* workReceiveEvent_;

  /// The socket used to send new work items to the event loop. It is
  /// expected that work items can always be read at once from it, i.e. that
  /// there will never be short reads.
  Socket workSendSocket_;

  /// Queued up work delegates for async transports (also includes currently
  /// active ones, they are removed from the queue on completion).
  // TODO: This should really be of some queue type, not an array, but
  // std.container doesn't have anything.
  TAsyncWorkItem[][TAsyncTransport] workQueues_;
}
