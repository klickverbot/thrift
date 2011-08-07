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

import core.atomic;
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
 * Once a operation is completed, the result of the operation can be fetched
 * via the get() family of methods. There are three possible cases: Either the
 * operation succeeded, then its return value is returned, or it failed by
 * throwing, in which case the exception is rethrown, or it was cancelled
 * before, then a TOperationCancelledException is thrown.
 *
 * All methods are thread-safe, but keep in mind that any exception object or
 * result (if it is a reference type, of course) is shared between all
 * get()-family invocations.
 */
interface TFuture(ResultType) {
  ///
  enum Status : byte {
    RUNNING, /// The operation is still running.
    SUCCEEDED, /// The operation completed without throwing an exception.
    FAILED, /// The operation completed by throwing an exception.
    CANCELLED /// The operation was cancelled.
  }

  /**
   * The status the operation is currently in.
   *
   * An operation starts out in RUNNING state, and changes state to one of the
   * others at most once afterwards.
   */
  Status status() const @property;

  /**
   * Waits until the result is available.
   *
   * Calling wait() on an operation that is already completed is a no-op.
   */
  void wait() out {
    // DMD @@BUG6108@.
    version(none) assert(state != RUNNING);
  }

 /**
  * Waits until the result is available, or the specified timeout expired.
  *
  * Calling wait() on an operation that is already completed is a no-op.
  *
  * Returns: false if the operation was still running after waiting the
  *   specified duration, true otherwise.
  */
  bool wait(Duration timeout) out (result) {
    // DMD @@BUG6108@.
    version(none) assert(!result || state != RUNNING);
  }

  /**
   * Waits until the result is available and returns it (resp. throws an
   * exception for the failed/cancelled cases).
   *
   * If the operation has already completed, the result is immediately
   * returned.
   *
   * The result of this method is »alias this«'d to the interface, so that
   * TFuture can be used as a drop-in replacement for a simple value in
   * synchronous code.
   */
  ResultType waitGet();
  alias waitGet this;

  /**
   * Waits until the result is available or the timeout expires.
   *
   * If the operation completes in time, returns its result (resp. throws an
   * exception for the failed/cancelled cases). If not, throws a
   * TFutureException.
   */
  ResultType waitGet(Duration timeout);

  /**
   * Returns the result of the operation.
   *
   * Throws: TFutureException if the operation has been cancelled,
   *   TOperationCancelledException if it is not yet done; the set exception
   *   if it failed.
   */
  ResultType get();

  /**
   * Returns the captured exception if the operation failed, or null otherwise.
   *
   * Throws: TFutureException if not yet done, TOperationCancelledException
   *   if the operation has been cancelled.
   */
  Exception getException();

  /**
   * Requests cancellation for the operation, i.e. indicates that the result
   * will not be needed and the operation can be stopped to free resources,
   * if possible.
   *
   * If cancel() is called on a not yet completed operation, subsequent calls
   * to the get() family of functions are expected, but not required, to fail
   * with a TOperationCancelledException. If it is called on an already
   * compled operation, nothing happens.
   */
  void cancel();
}

/**
 * A TFuture covering the simple but common case where the result is simply
 * set by a call to succeed()/fail().
 *
 * All methods are thread-safe, but usually, succeed()/fail() are only called
 * from a single thread (different from the thread(s) waiting for the result
 * using the TFuture interface, though).
 */
class TPromise(ResultType) : TFuture!ResultType {
  this() {
    statusMutex_ = new Mutex;
    statusCondition_ = new Condition(statusMutex_);
  }

  override void wait() {
    synchronized (statusMutex_) {
      while (atomicLoad(status_) == Status.RUNNING) statusCondition_.wait();
    }
  }

  override bool wait(Duration timeout) {
    synchronized (statusMutex_) {
      statusCondition_.wait(timeout);
    }
    // Make return value depend on the actual status_ instead of the return
    // value of Condition.wait() so that we never return true if the result
    // is not available, even in case of spurious wakeups. I am not sure if
    // they can actually happen for timed waits as well, but in any case they
    // should be rare enough to not warrant more expensive timeout checking.
    return atomicLoad(status_) != Status.RUNNING;
  }

  override ResultType waitGet() {
    wait();
    return get();
  }

  override ResultType waitGet(Duration timeout) {
    enforce(wait(timeout), new TFutureException(
      "Operation did not complete in time."));
    return get();
  }

  override Status status() const @property {
    return atomicLoad(status_);
  }

  override ResultType get() {
    auto status = atomicLoad(status_);
    enforce(status != Status.RUNNING,
      new TFutureException("Operation not yet completed."));

    if (status == Status.CANCELLED) throw new TOperationCancelledException;
    if (status == Status.FAILED) throw exception_;

    static if (!is(ResultType == void)) {
      return result_;
    }
  }

  override Exception getException() {
    auto status = atomicLoad(status_);
    enforce(status == Status.RUNNING,
      new TFutureException("Operation not yet completed."));

    if (status == Status.CANCELLED) throw new TOperationCancelledException;

    return exception_;
  }

  override void cancel() {
    synchronized (statusMutex_) {
      cas(&status_, Status.RUNNING, Status.CANCELLED);
    }
  }

  static if (!is(ResultType == void)) {
    /**
     * Sets the result of the operation, marks it as done, and notifies any
     * waiters.
     *
     * If the operation has been cancelled before, nothing happens.
     *
     * Throws: TFutureException if the operation is already completed.
     */
    void succeed(ResultType result) {
      synchronized (statusMutex_) {
        auto status = atomicLoad(status_);
        if (status == Status.CANCELLED) return;

        enforce(status == Status.RUNNING,
          new TFutureException("Operation already done."));
        result_ = result;

        atomicStore(status_, Status.SUCCEEDED);
        statusCondition_.notifyAll();
      }
    }
  } else {
    void succeed() {
      synchronized (statusMutex_) {
        auto status = atomicLoad(status_);
        if (status == Status.CANCELLED) return;

        enforce(status == Status.RUNNING,
          new TFutureException("Operation already done."));

        atomicStore(status_, Status.SUCCEEDED);
        statusCondition_.notifyAll();
      }
    }
  }

  /**
   * Marks the operation as failed with the specified exception and notifies
   * any waiters.
   *
   * If the operation was already cancelled, nothing happens.
   *
   * Throws: TFutureException if the operation is already completed.
   */
  void fail(Exception exception) {
    synchronized (statusMutex_) {
      auto status = atomicLoad(status_);
      if (status == Status.CANCELLED) return;

      enforce(status == Status.RUNNING,
        new TFutureException("Operation already done."));
      exception_ = exception;

      atomicStore(status_, Status.FAILED);
      statusCondition_.notifyAll();
    }
  }

private:
  shared Status status_;
  union {
    static if (!is(ResultType == void)) {
      ResultType result_;
    }
    Exception exception_;
  }

  Mutex statusMutex_;
  Condition statusCondition_;
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

///
class TOperationCancelledException : TException {
  ///
  this(string msg = "", string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    super(msg, file, line, next);
  }
}
