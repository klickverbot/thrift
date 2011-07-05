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
module thrift.server.taskpool;

// stderr is used for error messages until something more sophisticated is
// implemented.
import std.stdio : stderr;

import core.sync.condition;
import core.sync.mutex;
import std.exception : enforce;
import std.parallelism;
import thrift.base;
import thrift.protocol.base;
import thrift.protocol.processor;
import thrift.server.base;
import thrift.server.transport.base;
import thrift.transport.base;

/**
 * A server which dispatches client request to a std.parallelism TaskPool.
 */
class TTaskPoolServer : TServer {
  this(
    TProcessor processor,
    TServerTransport serverTransport,
    TTransportFactory transportFactory,
    TProtocolFactory protocolFactory
  ) {
    this(processor, serverTransport, transportFactory, transportFactory,
      protocolFactory, protocolFactory);
  }

  this(
    TProcessor processor,
    TServerTransport serverTransport,
    TTransportFactory inputTransportFactory,
    TTransportFactory outputTransportFactory,
    TProtocolFactory inputProtocolFactory,
    TProtocolFactory outputProtocolFactory
  ) {
    super(processor, serverTransport, inputTransportFactory,
      outputTransportFactory, inputProtocolFactory, outputProtocolFactory);
    taskPool_ = std.parallelism.taskPool;
  }

  override void serve() {
    TTransport client;
    TTransport inputTransport;
    TTransport outputTransport;
    TProtocol inputProtocol;
    TProtocol outputProtocol;

    try {
      // Start the server listening
      serverTransport.listen();
    } catch (TTransportException ttx) {
      stderr.writefln("TTaskPoolServer: listen() failed: %s", ttx);
      return;
    }

    if (eventHandler) eventHandler.preServe();

    auto queueState = QueueState();

    // Fetch client from server
    while (!stop_) {
      // Check if we can still handle more connections.
      if (maxActiveConns) {
        synchronized (queueState.mutex) {
          while (queueState.activeConns >= maxActiveConns) {
            queueState.connClosed.wait();
          }
        }
      }

      try {
        client = serverTransport.accept();
        scope(failure) client.close();

        inputTransport = inputTransportFactory.getTransport(client);
        scope(failure) inputTransport.close();

        outputTransport = outputTransportFactory.getTransport(client);
        scope(failure) outputTransport.close();

        inputProtocol = inputProtocolFactory.getProtocol(inputTransport);
        outputProtocol = outputProtocolFactory.getProtocol(outputTransport);
      } catch (TTransportException ttx) {
        if (!stop_) stderr.writefln(
          "TTaskPoolServer: TServerTransport died on accept: %s", ttx);
        continue;
      } catch (TException tx) {
        stderr.writefln("TTaskPoolServer: Caught TException on accept: %s", tx);
        continue;
      } catch (Exception e) {
        stderr.writefln(
          "TTaskPoolServer: Unknown exception on accept, stopping: %s", e);
        break;
      }

      synchronized (queueState.mutex) {
        ++queueState.activeConns;
      }
      taskPool_.put(task!worker(queueState, client, inputProtocol,
        outputProtocol, processor, eventHandler));
    }

    if (stop_) {
      // First, stop accepting new connections.
      try {
        serverTransport.close();
      } catch (TTransportException ttx) {
        stderr.writefln(
          "TTaskPoolServer: TServerTransport failed on close: %s", ttx);
      }

      // Then, wait until all active connections are finished.
      synchronized (queueState.mutex) {
        while (queueState.activeConns > 0) {
          queueState.connClosed.wait();
        }
      }

      stop_ = false;
    }
  }

  override void stop() {
    stop_ = true;
    serverTransport.interrupt();
  }

  /**
   * Sets the task pool to use.
   *
   * By default, the global std.parallelism taskPool instance is used, which
   * might not be appropriate for many applications, e.g. where tuning the
   * number of worker threads is desired.
   *
   * Note: TTaskPoolServer expects that tasks are never dropped from the pool,
   * e.g. by calling TaskPool.close() while there are still tasks in the
   * queue. If this happens, serve() will never return.
   */
  void setTaskPool(TaskPool pool) {
    enforce(pool.size > 0, "Cannot use a task pool with no worker threads.");
    taskPool_ = pool;
  }

  /**
   * The maximum number of client connections open at the same time. Zero for
   * no limit, which is the default.
   *
   * If this limit is reached, no clients are accept()ed from the server
   * transport any longer until a connection has been closed again.
   */
  size_t maxActiveConns;

protected:
  bool stop_;
  TaskPool taskPool_;
}

// Cannot be private as worker has to be passed as alias parameter to
// another module.
// private {
  /*
   * The state of the »connection queue«, i.e. used for keeping track of how
   * many client connections are currently processed.
   */
  struct QueueState {
    /// Protects the queue state.
    Mutex mutex;

    /// The number of active connections (from the time they are accept()ed
    /// until they are closed when the worked task finishes).
    size_t activeConns;

    /// Signals that the number of active connections has been decreased, i.e.
    /// that a connection has been closed.
    Condition connClosed;

    /// Returns an initialized instance.
    static QueueState opCall() {
      QueueState q;
      q.mutex = new Mutex;
      q.connClosed = new Condition(q.mutex);
      return q;
    }
  }

  void worker(ref QueueState queueState, TTransport client,
    TProtocol inputProtocol, TProtocol outputProtocol,
    TProcessor processor, TServerEventHandler eventHandler)
  {
    scope (exit) {
      synchronized (queueState.mutex) {
        assert(queueState.activeConns > 0);
        --queueState.activeConns;
        queueState.connClosed.notifyAll();
      }
    }

    Variant connectionContext;
    if (eventHandler) {
      connectionContext =
        eventHandler.createContext(inputProtocol, outputProtocol);
    }

    try {
      while (true) {
        if (eventHandler) {
          eventHandler.preProcess(connectionContext, client);
        }

        if (!processor.process(inputProtocol, outputProtocol,
          connectionContext) || !inputProtocol.transport.peek()
        ) {
          // Something went fundamentlly wrong or there is nothing more to
          // process, close the connection.
          break;
        }
      }
    } catch (TTransportException ttx) {
      stderr.writefln("TTaskPoolServer: Client died: %s", ttx);
    } catch (Exception e) {
      stderr.writefln("TTaskPoolServer: Uncaught exception: %s", e);
    }

    if (eventHandler) {
      eventHandler.deleteContext(connectionContext, inputProtocol,
        outputProtocol);
    }

    try {
      inputProtocol.transport.close();
    } catch (TTransportException ttx) {
      stderr.writefln("TTaskPoolServer: Input close failed: %s", ttx);
    }
    try {
      outputProtocol.transport.close();
    } catch (TTransportException ttx) {
      stderr.writefln("TTaskPoolServer: Output close failed: %s", ttx);
    }
    try {
      client.close();
    } catch (TTransportException ttx) {
      stderr.writefln("TTaskPoolServer: Client close failed: %s", ttx);
    }
  }
// }
