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

/**
 * Contains support infrastructure for handling coroutine-based asynchronous/
 * non-blocking operations, as used by thrift.codegen.TAsyncClient.
 *
 * The main piece of the »client side« (e.g. for TAsyncClient users) of the
 * API is TFuture, which represents an asynchronously executed operation,
 * which can have a return value, throw exceptions, and which can be waited
 * upon.
 *
 * On the »implementation side«, the idea is that by using a TAsyncTransport
 * instead of a normal TTransport and executing the work through a
 * TAsyncManager, the same code as for synchronous I/O can be reused in
 * between them, for example:
 *
 * ---
 * auto asyncManager = someTAsyncSocketManager();
 * auto socket = new TAsyncSocket(asyncManager, host, port);
 * …
 * asyncManager.execute(TAsyncWorkItem(socket, {
 *   SomeThriftStruct s;
 *   s.read(socket);
 *   // Do something with s, e.g. set a TPromise result to it.
 * });
 * ---
 */
module thrift.async.base;

import core.sync.condition;
import core.sync.mutex;
import core.time : Duration, dur;
import std.socket;
import thrift.base;
import thrift.transport.base;

/**
 * Manages one or more asynchronous transport resources (e.g. sockets in the
 * case of TAsyncSocketManager) and allows work items to be submitted for them.
 *
 * Implementations will typically run a background thread for executing the
 * work, which is one of the reasons for a TAsyncManager to be used. Each work
 * item is run in its own fiber and is expected to yield() away while it is
 * waiting for a time-consuming operation.
 *
 * The second important purpose of TAsyncManager is to serialize access to
 * the transport resources – without taking care of that, things would go
 * horribly wrong for example when issuing multiple RPC calls over the same
 * connection in rapid succession, so that more than one piece of client code
 * tries to write to the socket at the same time.
 *
 * All methods are thread-safe.
 */
interface TAsyncManager {
  /**
   * Submits a work item to be executed asynchronously.
   *
   * Note: The work item will likely be executed in a different thread, so make
   *   sure the code it relies on is thread-safe. An exception are the async
   *   transports themselves, to which access is serialized, as noted above.
   */
  void execute(TAsyncWorkItem work);

  /**
   * Shuts down all background threads or other facilities that might have
   * been started in order to execute work items. This function is typically
   * called during program shutdown.
   *
   * If there are still tasks to be executed when the timeout expires, any
   * currently executed work items will never receive any notifications
   * for async transports managed by this instance, and queued work items will
   * be silently dropped.
   *
   * Params:
   *   waitFinishTimeout = If positive, waits for all work items to be
   *     finished for the specified amount of time, if negative, waits for
   *     completion without ever timing out, if zero, immediately shuts down
   *     the background facilities.
   */
  bool stop(Duration waitFinishTimeout = dur!"hnsecs"(-1));
}

/**
 * A transport which uses a TAsyncManager to schedule non-blocking operations.
 */
interface TAsyncTransport : TTransport {
  /**
   * The TAsyncManager associated with this transport.
   */
  TAsyncManager asyncManager() @property;
}

/**
 * A work item operating on an TAsyncTransport, which can be executed
 * by a TAsyncManager.
 */
struct TAsyncWorkItem {
  /// The async transport the task to execute operates on.
  TAsyncTransport transport;

  /// The task to execute. While pretty much anything could be passed in
  /// theory, it should be something which relies on the given transport
  /// in practice for the concept to make sense.
  /// Note: Executing the task should never throw, errors should be handled
  /// in another way. nothrow semantics are difficult to enforce in combination
  /// with fibres though, so exceptions are typically just swallowed by
  /// TAsyncManager implementations.
  void delegate() work;
}

/**
 * A TAsyncManager supporting async operations/notifications for sockets.
 */
interface TAsyncSocketManager : TAsyncManager {
  /**
   * Adds a listener that is triggered once when an event of the specified type
   * occurs, and removed afterwards.
   *
   * Params:
   *   socket = The socket to listen for events at.
   *   eventType = The type of the event to listen for.
   *   timeout = The period of time after which the listener will be called
   *     with TAsyncEventReason.TIMED_OUT if no event happened.
   *   listener = The delegate to call when an event happened.
   */
  void addOneshotListener(Socket socket, TAsyncEventType eventType,
    Duration timeout, SocketEventListener listener);

  /// Ditto
  void addOneshotListener(Socket socket, TAsyncEventType eventType,
    SocketEventListener listener);
}

/**
 * Types of events that can happen for an asynchronous transport.
 */
enum TAsyncEventType {
  READ, /// New data became available to read.
  WRITE /// The transport became ready to be written to.
}

/**
 * The type of the delegates used to register socket event handlers.
 */
alias void delegate(TAsyncEventReason callReason) SocketEventListener;

/**
 * The reason a listener was called.
 */
enum TAsyncEventReason : byte {
  NORMAL, /// The event listened for was triggered normally.
  TIMED_OUT /// A timeout for the event was set, and it expired.
}

/**
 * Represents an operation which is executed asynchronously and the result of
 * which will become available at some point in the future.
 *
 * All methods are thread-safe.
 *
 * Note: Currently, no support for canceling operations is implemented yet,
 *   although this will likely change soon.
 */
interface TFuture(ResultType) {
  /**
   * Whether the result is already available.
   */
  bool done() const @property;

  /**
   * Waits until the operation is completed.
   *
   * The result is guaranteed to be available afterwards.
   */
  void wait() out {
    // DMD @@BUG6108@.
    version(none) assert(done);
  }

 /**
  * Waits until the operation is completed or the specified timeout expired.
  *
  * Returns: true if the result became available in time (done is guaranteed
  *   to be set then), false otherwise.
  */
  bool wait(Duration timeout) out (result) {
    // DMD @@BUG6108@.
    version(none) assert(!result || done);
  }

  /**
   * Waits until the operation is completed and returns its result, or
   * rethrows any exception if it fails.
   *
   * The result of this method is »alias this«'d to the interface, so that
   * TFuture can be used as a drop-in replacement for a simple value in
   * synchronous code.
   */
  ResultType waitGet();
  alias waitGet this;

  /**
   * Waits until the operation is completed or the timeout expires.
   *
   * If the operation is completed in time, returns its result, or rethrows
   * any exception if it failed. If not, throws a TFutureException.
   */
  ResultType waitGet(Duration timeout);

  /**
   * Returns the result of the operation.
   *
   * Throws: TFutureException if not yet done; the set exception if any.
   */
  ResultType get();

  /**
   * Returns the captured exception if the operation failed, or null otherwise.
   *
   * Throws: TFutureException if not yet done.
   */
  Exception getException();
}

/**
 * A TFuture covering the simple but common case where the result is simply
 * set by a call to complete()/fail().
 *
 * All methods are thread-safe, but usually, complete()/fail() are only called
 * from a single thread (different from the thread(s) waiting for the result
 * using the TFuture interface).
 */
class TPromise(ResultType) : TFuture!ResultType {
  this() {
    doneMutex_ = new Mutex;
    doneCondition_ = new Condition(doneMutex_);
  }

  /+override+/ void wait() {
    // If we are already done, return early to avoid needlessly acquiring the
    // lock.
    if (done_) return;

    synchronized (doneMutex_) {
      while (!done_) doneCondition_.wait();
    }
  }

  /+override+/ bool wait(Duration timeout) {
    // If we are already done, return early to avoid needlessly acquiring the
    // lock.
    if (done_) return true;

    synchronized (doneMutex_) {
      doneCondition_.wait(timeout);
    }

    // Return done_ instead of directly the return value of Condition.wait()
    // so that we never return true if the result is not available, even in
    // case of spurious wakeups. I am not sure if they can actually happen for
    // a timed wait as well, but in any case they should be rare enough to not
    // warrant more expensive timeout checking (e.g. calculating the expected
    // wakeup time from the current system clock would be possible).
    return done_;
  }

  /+override+/ ResultType waitGet() {
    wait();

    if (exception_) throw exception_;
    static if (!is(ResultType == void)) {
      return result_;
    }
  }

  /+override+/ ResultType waitGet(Duration timeout) {
    enforce(wait(timeout), new TFutureException(
      "Result was not available in time."));

    if (exception_) throw exception_;
    static if (!is(ResultType == void)) {
      return result_;
    }
  }

  /+override+/ bool done() const @property {
    return done_;
  }

  /+override+/ ResultType get() {
    enforce(done_, new TFutureException("Result not yet available."));
    if (exception_) throw exception_;
    static if (!is(ResultType == void)) {
      return result_;
    }
  }

  static if (!is(ResultType == void)) {
    /**
     * Sets the result of the operation, marks it as done, and notifies any
     * waiters.
     *
     * Throws: TFutureException if the operation is already completed.
     */
    void complete(ResultType result) {
      synchronized (doneMutex_) {
        enforce(!done_, new TFutureException("Operation already done."));
        result_ = result;
        done_ = true;
        doneCondition_.notifyAll();
      }
    }
  } else {
    void complete() {
      synchronized (doneMutex_) {
        enforce(!done_, new TFutureException("Operation already done."));
        done_ = true;
        doneCondition_.notifyAll();
      }
    }
  }

  /+override+/ Exception getException() {
    enforce(done_, new TFutureException("Result not yet available."));
    return exception_;
  }

  /**
   * Marks the operation as failed with the specified exception and notifies
   * any waiters.
   *
   * Throws: TFutureException if the operation is already completed.
   */
  void fail(Exception exception) {
    synchronized (doneMutex_) {
      enforce(!done_, new TFutureException("Operation already done."));
      exception_ = exception;
      done_ = true;
      doneCondition_.notifyAll();
    }
  }

private:
  shared bool done_;
  static if (!is(ResultType == void)) {
    ResultType result_;
  }
  Exception exception_;

  Mutex doneMutex_;
  Condition doneCondition_;
}

///
class TFutureException : TException {
  ///
  this(string msg = "", string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    super(msg, file, line, next);
  }
}

