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
module thrift.async.base;

import core.sync.condition;
import core.sync.mutex;
import std.socket;
import thrift.base;
import thrift.transport.base;

interface TAsyncTransport : TTransport {
  TAsyncManager asyncManager() @property;
}

interface TAsyncManager {
  void execute(TAsyncWorkItem work);
  TAsyncSocketManager socketManager() @property;
}

struct TAsyncWorkItem {
  TAsyncTransport transport;
  Work work;
}

alias void delegate() Work;

interface TAsyncSocketManager {
  void addOneshotListener(Socket socket, TAsyncEventType eventType,
    SocketEventListener listener);
}

enum TAsyncEventType {
  READ,
  WRITE
}

alias void delegate() SocketEventListener;

final class TFuture(ResultType) {
  this() {
    doneMutex_ = new Mutex;
    doneCondition_ = new Condition(doneMutex_);
  }

  /**
   * Waits until the operation is completed and returns its result, or
   * rethrows any exception if it fails.
   */
  ResultType await() {
    if (!done_) {
      synchronized (doneMutex_) {
        while (!done_) doneCondition_.wait();
      }
    }

    if (exception_) throw exception_;

    static if (!is(ResultType == void)) {
      return result_;
    }
  }

  alias await this;

  /**
   * Whether the result is already available.
   */
  bool done() const @property {
    return done_;
  }

  static if (!is(ResultType == void)) {
    /**
     * Returns the result of the operation.
     *
     * Throws: TFutureException if not yet done; the set exception if any.
     */
    ResultType result() const @property {
      enforce(done_, new TFutureException("Result not yet available."));
      if (exception_) throw exception_;
      return result_;
    }

    /**
     * Sets the result of the operation, marks it as done, and notifies any
     * waiters.
     *
     * Throws: TFutureException if the operation is already completed.
     */
    void complete(ResultType result) {
      enforce(!done_, new TFutureException("Operation already done."));
      result_ = result;
      notifyCompletion();
    }
  } else {
    void complete() {
      enforce(!done_, new TFutureException("Operation already done."));
      notifyCompletion();
    }
  }

  /**
   * Marks the operation as failed with the specified exception and notifies
   * any waiters.
   *
   * Throws: TFutureException if the operation is already completed.
   */
  void fail(Exception exception) {
    enforce(!done_, new TFutureException("Operation already done."));
    exception_ = exception;
    notifyCompletion();
  }

private:
  void notifyCompletion() {
    assert(!done_);
    synchronized (doneMutex_) {
      doneCondition_.notifyAll();
      done_ = true;
    }
  }

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

