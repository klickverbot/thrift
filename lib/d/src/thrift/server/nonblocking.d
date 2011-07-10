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
 * A non-blocking server implementation that operates a single »acceptor«
 * thread and either does processing »in-line« or off-loads it to a task pool.
 *
 * It *requires* TFramedTransport to be used on the client side, as it expects
 * a 4 byte length indicator and writes out responses using the same framing.
 *
 * Because I/O is done asynchronous/event based, unfortunately
 * TServerTransport can't be used – the TServer.serverTransport property will
 * always be null.
 *
 * This implementation is closely based on the C++ one, with the exception
 * of request timeouts and the drain task queue overload handling strategy not
 * being implemented yet.
 */
// TODO: This really should use a D non-blocking I/O library, once one becomes
// available.
module thrift.server.nonblocking;

import core.memory : GC;
import core.stdc.errno : EAGAIN, EWOULDBLOCK;
import core.time : Duration, dur;
import std.conv : emplace, to;
import std.parallelism : TaskPool, task;
import std.socket;
import std.stdio : stdout, stderr; // No proper logging support yet.
import thrift.base;
import thrift.protocol.base;
import thrift.protocol.binary;
import thrift.protocol.processor;
import thrift.server.base;
import thrift.server.transport.socket;
import thrift.transport.base;
import thrift.transport.memory;
import thrift.transport.range;
import thrift.transport.socket;
import thrift.util.event.event;
import thrift.util.event.event_compat;

/**
 * Possible actions taken on new incoming connections when the server is
 * overloaded.
 */
enum TOverloadAction {
  /// Do not take any special actions while the server is overloaded, just
  /// continue accepting connections.
  NONE,

  /// Immediately drop new connections after they have been accepted if the
  /// server is overloaded.
  CLOSE_ON_ACCEPT
}

///
class TNonblockingServer : TServer {
  ///
  this(TProcessor processor, ushort port) {
    this(processor, port, new TTransportFactory, new TBinaryProtocolFactory!());
  }

  ///
  this(TProcessor processor, ushort port, TTransportFactory transportFactory,
    TProtocolFactory protocolFactory, TaskPool taskPool = null)
  {
    this(processor, port, transportFactory, transportFactory, protocolFactory,
      protocolFactory, taskPool);
  }

  ///
  this(
    TProcessor processor,
    ushort port,
    TTransportFactory inputTransportFactory,
    TTransportFactory outputTransportFactory,
    TProtocolFactory inputProtocolFactory,
    TProtocolFactory outputProtocolFactory,
    TaskPool taskPool = null
  ) {
    super(processor, null, inputTransportFactory, outputTransportFactory,
      inputProtocolFactory, outputProtocolFactory);
    port_ = port;
    this.taskPool = taskPool;

    if (taskPool) {
      stdout.writefln("TNonblockingServer: Using task pool with size: %s",
        taskPool.size);
    }

    connectionStackLimit = DEFAULT_CONNECTION_STACK_LIMIT;
    maxActiveProcessors = DEFAULT_MAX_ACTIVE_PROCESSORS;
    maxConnections = DEFAULT_MAX_CONNECTIONS;
    overloadHysteresis = DEFAULT_OVERLOAD_HYSTERESIS;
    overloadAction = DEFAULT_OVERLOAD_ACTION;
    writeBufferDefaultSize = DEFAULT_WRITE_BUFFER_DEFAULT_SIZE;
    idleReadBufferLimit = DEFAULT_IDLE_READ_BUFFER_LIMIT;
    idleWriteBufferLimit = DEFAULT_IDLE_WRITE_BUFFER_LIMIT;
    resizeBufferEveryN = DEFAULT_RESIZE_BUFFER_EVERY_N;
  }

  ~this() {
    if (eventBase_) {
      event_base_free(eventBase_);
    }
  }

  override void serve() {
    // Initialize the listening socket.
    // TODO: SO_KEEPALIVE, TCP_LOW_MIN_RTO, etc.
    listenSocket_ = makeSocketAndListen(port_, TServerSocket.ACCEPT_BACKLOG,
      BIND_RETRY_LIMIT, BIND_RETRY_DELAY);
    listenSocket_.blocking = false;

    if (taskPool) {
      auto pair = socketPair();
      foreach (s; pair) s.blocking = false;
      completionSendSocket_ = pair[0];
      completionReceiveSocket_ = pair[1];
    }

    // Initialize libevent core
    registerEvents(event_init());

    // Run libevent engine, never returns, invokes calls to eventHandler
    if (eventHandler) eventHandler.preServe();
    event_base_loop(eventBase_, 0);
  }

  /**
   * Returns the number of currently active connections, i.e. open sockets.
   */
  size_t getNumConnections() const {
    return numConnections_;
  }

  /**
   * Returns the number of connection objects allocated, but not in use.
   */
  size_t getNumIdleConnections() const {
    return connectionStack_.length;
  }

  /**
   * Return count of number of connections which are currently processing.
   *
   * This is defined as a connection where all data has been received, and the
   * processor was invoked but has not yet completed.
   */
  size_t getNumActiveProcessors() const {
    return numActiveProcessors_;
  }

  /// Number of bind() retries.
  enum BIND_RETRY_LIMIT = 0;

  /// Duration between bind() retries.
  enum BIND_RETRY_DELAY = dur!"hnsecs"(0);

  /// The task pool to use for processing requests. If null, no additional
  /// threads are used and request are processed »inline«.
  TaskPool taskPool;

  /**
   * Hysteresis for overload state.
   *
   * This is the fraction of the overload value that needs to be reached
   * before the overload state is cleared. It must be between 0 and 1,
   * practical choices probably lie between 0.5 and 0.9.
   */
  double overloadHysteresis() const @property {
    return overloadHysteresis_;
  }

  /// Ditto
  void overloadHysteresis(double value) @property {
    enforce(0 < value && value <= 1,
      "Invalid value for overload hysteresis: " ~ to!string(value));
    overloadHysteresis_ = value;
  }

  /// Ditto
  enum DEFAULT_OVERLOAD_HYSTERESIS = 0.8;

  /**
   * The action which will be taken on overload.
   */
  TOverloadAction overloadAction;

  /// Ditto
  enum DEFAULT_OVERLOAD_ACTION = TOverloadAction.NONE;

  /**
   * The write buffer is initialized (and when idleWriteBufferLimit_ is checked
   * and found to be exceeded, reinitialized) to this size.
   */
  size_t writeBufferDefaultSize;

  /// Ditto
  enum size_t DEFAULT_WRITE_BUFFER_DEFAULT_SIZE = 1024;

  /**
   * Max read buffer size for an idle Connection. When we place an idle
   * Connection into connectionStack_ or on every resizeBufferEveryN_ calls,
   * we will free the buffer (such that it will be reinitialized by the next
   * received frame) if it has exceeded this limit. 0 disables this check.
   */
  size_t idleReadBufferLimit;

  /// Ditto
  enum size_t DEFAULT_IDLE_READ_BUFFER_LIMIT = 1024;

  /**
   * Max write buffer size for an idle connection.  When we place an idle
   * Connection into connectionStack_ or on every resizeBufferEveryN_ calls,
   * we ensure that its write buffer is <= to this size; otherwise we
   * replace it with a new one of writeBufferDefaultSize_ bytes to ensure that
   * idle connections don't hog memory. 0 disables this check.
   */
  size_t idleWriteBufferLimit;

  /// Ditto
  enum size_t DEFAULT_IDLE_WRITE_BUFFER_LIMIT = 1024;

  /**
   * Every N calls we check the buffer size limits on a connected Connection.
   * 0 disables (i.e. the checks are only done when a connection closes).
   */
  uint resizeBufferEveryN;

  /// Ditto
  enum uint DEFAULT_RESIZE_BUFFER_EVERY_N = 512;

  /// Limit for how many Connection objects to cache.
  size_t connectionStackLimit;

  /// Ditto
  enum size_t DEFAULT_CONNECTION_STACK_LIMIT = 1024;

  /// Limit for number of open connections before server goes into overload
  /// state.
  size_t maxConnections;

  /// Ditto
  enum size_t DEFAULT_MAX_CONNECTIONS = int.max;

  /// Limit for number of connections processing or waiting to process
  size_t maxActiveProcessors;

  /// Ditto
  enum size_t DEFAULT_MAX_ACTIVE_PROCESSORS = int.max;

private:
  /**
   * Called when server socket had something happen.  We accept all waiting
   * client connections on listen socket fd and assign Connection objects
   * to handle those requests.
   */
  void handleEvent(int fd, short which) {
    // Make sure that libevent didn't mess up the socket handles
    assert(fd == listenSocket_.handle);

    // Accept as many new clients as possible, even though libevent signaled
    // only one. This helps the number of calls into libevent space.
    while (true) {
      // It is lame to use exceptions for regular control flow (failing is
      // excepted due to non-blocking mode of operation), but that's the
      // interface std.socket offers…
      Socket clientSocket;
      try {
        clientSocket = listenSocket_.accept();
      } catch (SocketAcceptException e) {
        if (!(e.errorCode == EWOULDBLOCK || e.errorCode == EAGAIN)) {
          stderr.writefln("TNonblockingServer.handleEvent(): Error " ~
            "accepting conection: %s", e);
        }
        break;
      }

      // If the server is overloaded, this is the point to take the specified
      // action.
      if (overloadAction != TOverloadAction.NONE && checkOverloaded()) {
        nConnectionsDropped_++;
        nTotalConnectionsDropped_++;
        if (overloadAction == TOverloadAction.CLOSE_ON_ACCEPT) {
          clientSocket.close();
          return;
        }
      }

      try {
        clientSocket.blocking = false;
      } catch (SocketException e) {
        stderr.writefln("TNonblockingServer.handleEvent(): Couldn't set " ~
          "client socket to non-blocking mode: %s", e);
        clientSocket.close();
        return;
      }

      // Create a new Connection for this client socket.
      auto conn = createConnection(clientSocket, EV_READ | EV_PERSIST);

      // Initialize the connection.
      conn.transition();
    }
  }

  /// Increment the count of connections currently processing.
  void incrementActiveProcessors() {
    ++numActiveProcessors_;
  }

  /// Decrement the count of connections currently processing.
  void decrementActiveProcessors() {
    assert(numActiveProcessors_ > 0);
    --numActiveProcessors_;
  }

  /**
   * Determines if the server is currently overloaded.
   *
   * If the number of open connections or »processing« connections is over the
   * respective limit, the server will enter overload handling mode and a
   * warning will be logged. If below values are below the hysteresis curve,
   * this will cause the server to exit it again.
   *
   * Returns: Whether the server is currently overloaded.
   */
  bool checkOverloaded() {
    auto activeConnections = numConnections_ - connectionStack_.length;
    if (numActiveProcessors_ > maxActiveProcessors ||
        activeConnections > maxConnections) {
      if (!overloaded_) {
        stdout.writeln("TNonblockingServer: Entering overloaded state.");
        overloaded_ = true;
      }
    } else {
      if (overloaded_ &&
          (numActiveProcessors_ <= overloadHysteresis_ * maxActiveProcessors) &&
          (activeConnections <= overloadHysteresis_ * maxConnections)) {
        stdout.writefln("TNonblockingServer: Exiting overloaded state, %s " ~
          "connections dropped (% total).", nConnectionsDropped_,
          nTotalConnectionsDropped_);
        nConnectionsDropped_ = 0;
        overloaded_ = false;
      }
    }

    return overloaded_;
  }

  /**
   * Registers the needed libevent events on the given event_base.
   */
  void registerEvents(event_base* base) {
    assert(listenSocket_);
    assert(!eventBase_);
    eventBase_ = base;

    // Print some libevent stats
    stdout.writefln("TNonblockingServer: libevent version %s, using method %s",
      to!string(event_get_version()), to!string(event_get_method()));

    // Register the server event
    event_set(&listenEvent_, listenSocket_.handle, EV_READ | EV_PERSIST,
      &handleEventCallback, cast(void*)this);
    event_base_set(eventBase_, &listenEvent_);

    // Add the event and start up the server
    if (event_add(&listenEvent_, null) == -1) {
      throw new TException("event_add for the listening socket event failed.");
    }
    if (taskPool) {
      // Create an event to be notified when a task finishes
      event_set(&completionEvent_, completionReceiveSocket_.handle,
        EV_READ | EV_PERSIST, &taskCompletionCallback, cast(void*)this);

      // Attach to the base
      event_base_set(eventBase_, &completionEvent_);

      // Add the event and start up the server
      if (event_add(&completionEvent_, null) == -1) {
        throw new TException("event_add for the notification socket failed.");
      }
    }
  }

  /**
   * C-callable event handler for listener events.  Provides a callback
   * that libevent can understand which invokes server.handleEvent().
   */
  extern(C) static void handleEventCallback(int fd, short which, void* serverThis) {
    (cast(TNonblockingServer)serverThis).handleEvent(fd, which);
  }

  /**
   * C-callable event handler for signaling task completion.  Provides a
   * callback that libevent can understand that will read a connection
   * object's address from a pipe and call connection.transition() for
   * that object.
   */
  extern(C) static void taskCompletionCallback(int fd, short which, void* serverThis) {
    auto server = cast(TNonblockingServer) serverThis;

    Connection connection;
    ptrdiff_t bytesRead;
    while (true) {
      bytesRead = server.completionReceiveSocket_.receive(
        cast(ubyte[])((&connection)[0 .. 1]));
      if (bytesRead < 0) {
        auto errno = getSocketErrno();

        // TODO: Windows.
        if (!(errno == EWOULDBLOCK || errno == EAGAIN)) {
          stderr.writefln("TNonblockingServer.taskCompletionCallback(): read " ~
            "failed, resources will be leaked: %s", socketErrnoString(errno));
        }
      }

      if (bytesRead != Connection.sizeof) break;

      connection.transition();
    }

    if (bytesRead > 0) {
      stderr.writefln("TNonblockingServer.taskCompletionCallback(): " ~
        "Unexpected partial read (%s bytes instead of %s)",
        bytesRead, Connection.sizeof);
    }
  }

  /**
   * Returns an initialized connection object, reusing one from the idle
   * connection stack if possible.
   *
   * See: Connection.init().
   */
  Connection createConnection(Socket socket, short flags) {
    if (connectionStack_.empty) {
      ++numConnections_;
      auto result = new Connection(socket, flags, this);

      // Make sure the connection does not get collected while it is active,
      // i.e. hooked up with libevent.
      GC.addRoot(cast(void*)result);

      return result;
    } else {
      auto result = connectionStack_[$ - 1];
      connectionStack_ = connectionStack_[0 .. $ - 1];
      connectionStack_.assumeSafeAppend();
      result.init(socket, flags, this);
      return result;
    }
  }

  /**
   * Returns a connection to pool or deletion.  If the connection pool
   * (a stack) isn't full, place the connection object on it, otherwise
   * just delete it.
   */
  void disposeConnection(Connection connection) {
    if (!connectionStackLimit ||
      (connectionStack_.length < connectionStackLimit))
    {
      connection.checkIdleBufferLimit(idleReadBufferLimit,
        idleWriteBufferLimit);
      connectionStack_ ~= connection;
    } else {
      assert(numConnections_ > 0);
      --numConnections_;
    }

    // We no longer need the additional root – if the connection is cached, we
    // have already put on the connectionStack_, and if not, we leave it for
    // collection now.
    GC.removeRoot(cast(void*)connection);
  }

  /// Server socket file descriptor
  Socket listenSocket_;

  /// Port to listen on.
  ushort port_;

  /// The event base for libevent,
  event_base* eventBase_;

  /// The libevent event definition for the listening socket events.
  event listenEvent_;

  /// The libevent event definition for the connection completion events.
  event completionEvent_;

  /// The total number of connections existing, both active and idle.
  size_t numConnections_;

  /// The number of connections which are currently waiting for the processor
  /// to return.
  size_t numActiveProcessors_;

  /// Hysteresis for overload state to be cleared.
  double overloadHysteresis_;

  /// Whether the server is currently overloaded.
  bool overloaded_;

  /// Number of connections dropped since the server entered the current
  /// overloaded state.
  uint nConnectionsDropped_;

  /// Number of connections dropped due to overload since the server started.
  ulong nTotalConnectionsDropped_;

  /// Socket used to send completion notification messages. Paired with
  /// completionReceiveSocket_.
  Socket completionSendSocket_;

  /// Socket used to send completion notification messages. Paired with
  /// completionSendSocket_.
  Socket completionReceiveSocket_;

  /// All the connection objects which have been created but are not currently
  /// in use. When a connection is closed, it it placed here to enable object
  /// reuse.
  Connection[] connectionStack_;
}

private {
  /*
   * I/O states a socket can be in.
   */
  enum SocketState {
    RECV_FRAME_SIZE, /// The frame size is received.
    RECV, /// The payload is received.
    SEND /// The response is written back out.
  }

  /*
   * States a connection can be in.
   */
  enum ConnectionState {
    INIT, /// The connection will be initialized.
    READ_FRAME_SIZE, /// The four frame size bytes are being read.
    READ_REQUEST, /// The request payload itself is being read.
    WAIT_PROCESSOR, /// The connection waits for the processor to finish.
    SEND_RESULT /// The result is written back out.
  }

  /**
   * Represents a connection that is handled via libevent. This connection
   * essentially encapsulates a socket that has some associated libevent state.
   */
  final class Connection {
    /**
     * Constructs a new instance.
     *
     * To reuse a connection object later on, the init() function can be used
     * to the same effect on the internal state.
     */
    this(Socket socket, short eventFlags, TNonblockingServer s) {
      // Allocate input and output tranpsorts
      // these only need to be allocated once per Connection (they don't need to be
      // reallocated on init() call)
      inputTransport_ = new TInputRangeTransport!(ubyte[])([]);
      outputTransport_ = new TMemoryBuffer(s.writeBufferDefaultSize);

      init(socket, eventFlags, s);
    }

    /**
     * Initializes the connection.
     *
     * Params:
     *   socket = The socket to work on.
     *   eventFlags = Any flags to pass to libevent.
     *   s = The server this connection is part of.
     */
    void init(Socket socket, short eventFlags, TNonblockingServer s) {
      // TODO: This allocation could be avoided
      socket_ = new TSocket(socket);

      server_ = s;
      connState_ = ConnectionState.INIT;
      eventFlags_ = 0;

      readBufferPos_ = 0;
      readWant_ = 0;

      writeBuffer_ = null;
      writeBufferPos_ = 0;
      largestWriteBufferSize_ = 0;

      socketState_ = SocketState.RECV_FRAME_SIZE;
      connState_ = ConnectionState.INIT;
      callsSinceResize_ = 0;

      registerEvent(eventFlags);

      factoryInputTransport_ = s.inputTransportFactory.getTransport(inputTransport_);
      factoryOutputTransport_ = s.outputTransportFactory.getTransport(outputTransport_);

      inputProtocol_ = s.inputProtocolFactory.getProtocol(factoryInputTransport_);
      outputProtocol_ = s.outputProtocolFactory.getProtocol(factoryOutputTransport_);

      if (s.eventHandler) {
        connectionContext_ =
          s.eventHandler.createContext(inputProtocol_, outputProtocol_);
      }
    }

    ~this() {
      free(readBuffer_);
    }

    /**
     * Check buffers against the size limits and shrink them if exceeded.
     *
     * Params:
     *   readLimit = Read buffer size limit (in bytes, 0 to ignore).
     *   writeLimit = Write buffer size limit (in bytes, 0 to ignore).
     */
    void checkIdleBufferLimit(size_t readLimit, size_t writeLimit) {
      if (readLimit > 0 && readBufferSize_ > readLimit) {
        free(readBuffer_);
        readBuffer_ = null;
        readBufferSize_ = 0;
      }

      if (writeLimit > 0 && largestWriteBufferSize_ > writeLimit) {
        // just start over
        outputTransport_.reset(server_.writeBufferDefaultSize);
        largestWriteBufferSize_ = 0;
      }
    }

    /**
     * Transtitions the connection to the next state.
     *
     * This is called e.g. when the request has been read completely or all
     * the data has been written back.
     */
    void transition() {
      // Switch upon the state that we are currently in and move to a new state
      final switch (connState_) {
        case ConnectionState.READ_REQUEST:
          // We are done reading the request, package the read buffer into transport
          // and get back some data from the dispatch function
          inputTransport_.reset(readBuffer_[0 .. readBufferPos_]);
          outputTransport_.reset();

          // Prepend four bytes of blank space to the buffer so we can
          // write the frame size there later.
          // TODO: Strictly speaking, we wouldn't have to write anything, just
          // increment the TMemoryBuffer writeOffset_. This would yield a tiny
          // performance gain.
          ubyte[4] space = void;
          outputTransport_.write(space);

          server_.incrementActiveProcessors();

          if (server_.taskPool) {
            // Create a new task and add it to the task pool queue.
            auto processingTask = task!processRequest(this);
            connState_ = ConnectionState.WAIT_PROCESSOR;
            server_.taskPool.put(processingTask);

            // We don't want to process any more data while the task is active.
            unregisterEvent();
            return;
          }

          // Just process it right now if there is no task pool set.
          processRequest(this);
          goto case;
        case ConnectionState.WAIT_PROCESSOR:
          // We have now finished processing the request, set the frame size
          // for the outputTransport_ contents and set everything up to write
          // it out via libevent.
          server_.decrementActiveProcessors();

          // Acquire the data written to the transport.
          // KLUDGE: To avoid copying, we simply cast the const away and
          // modify the internal buffer of the TMemoryBuffer – works with the
          // current implementation, but isn't exactly beautiful.
          writeBuffer_ = cast(ubyte[])outputTransport_.getContents();
          if (writeBuffer_.empty) {
            // The request was one-way, no response to write.
            goto case ConnectionState.INIT;
          }

          // Write the frame size into the four bytes reserved for it.
          assert(writeBuffer_.length > 4);
          auto size = hostToNet(writeBuffer_.length - 4);
          writeBuffer_[0 .. 4] = cast(ubyte[])((&size)[0 .. 1]);

          writeBufferPos_ = 0;
          socketState_ = SocketState.SEND;
          connState_ = ConnectionState.SEND_RESULT;
          registerWriteEvent();

          return;
        case ConnectionState.SEND_RESULT:
          // The result has been sent back to the client, we don't need the
          // bufferes anymore.
          if (writeBuffer_.length > largestWriteBufferSize_) {
            largestWriteBufferSize_ = writeBuffer_.length;
          }

          if (server_.resizeBufferEveryN > 0 &&
              ++callsSinceResize_ >= server_.resizeBufferEveryN
          ) {
            checkIdleBufferLimit(server_.idleReadBufferLimit,
              server_.idleWriteBufferLimit);
            callsSinceResize_ = 0;
          }

          goto case;
        case ConnectionState.INIT:
          writeBuffer_ = null;
          writeBufferPos_ = 0;
          socketState_ = SocketState.RECV_FRAME_SIZE;
          connState_ = ConnectionState.READ_FRAME_SIZE;
          readBufferPos_ = 0;
          registerReadEvent();

          return;
        case ConnectionState.READ_FRAME_SIZE:
          // We just read the request length, set up the buffers for reading
          // the payload.
          if (readWant_ > readBufferSize_) {
            // The current buffer is too small, exponentially grow the buffer
            // until it is big enough.

            if (readBufferSize_ == 0) {
              readBufferSize_ = 1;
            }

            auto newSize = readBufferSize_;
            while (readWant_ > newSize) {
              newSize *= 2;
            }

            auto newBuffer = cast(ubyte*)realloc(readBuffer_, newSize);
            if (newBuffer is null) onOutOfMemoryError();

            readBuffer_ = newBuffer;
            readBufferSize_ = newSize;
          }

          readBufferPos_= 0;

          socketState_ = SocketState.RECV;
          connState_ = ConnectionState.READ_REQUEST;

          // TODO: If a request was already ready to receive from the socket,
          // we would probably never get notified of it again.

          return;
      }
    }

    /**
     * C callback to call workSocket() from libevent.
     *
     * We have set the opaque data pointer to this before.
     */
    extern(C) static void workSocketCallback(int, short, void* v) {
      (cast(Connection)v).workSocket();
    }

    /**
     * Notifies server that processing has ended on this request.
     *
     * Can be called either when processing is completed or when a waiting
     * task has been preemptively terminated (on overload).
     */
    void notifyServer() {
      if (!server_.taskPool) return;

      assert(server_.completionSendSocket_);
      auto bytesSent =
        server_.completionSendSocket_.send(cast(ubyte[])((&this)[0 .. 1]));

      if (bytesSent != Connection.sizeof) {
        stderr.writeln(
          "TNonblockingServer: Sending completion notification falied.");
      }
    }

  private:
    /**
     * Invoked by libevent when something happens on the socket.
     */
    void workSocket() {
      final switch (socketState_) {
        case SocketState.RECV_FRAME_SIZE:
          // If some bytes have already been read, they have been kept in
          // readWant_.
          auto frameSize = readWant_;

          try {
            // Read from the socket
            auto bytesRead = socket_.read(
              (cast(ubyte[])((&frameSize)[0 .. 1]))[readBufferPos_ .. $]);
            if (bytesRead == 0) {
              // Couldn't read anything, but we have been notified – client
              // has disconnected.
              close();
              return;
            }

            readBufferPos_ += bytesRead;
          } catch (TTransportException te) {
            stderr.writefln(
              "Connection.workSocket(): Failed to read frame size %s", te);
            close();
            return;
          }

          if (readBufferPos_ < frameSize.sizeof) {
            // Frame size not complete yet, save the current buffer in
            // readWant_ so that the remaining bytes can be read later.
            readWant_ = frameSize;
            return;
          }

          auto size = netToHost(frameSize);
          if (size <= 0) {
            stderr.writefln("Connection.workSocket(): Negative frame size " ~
              "(%s), client not using TFramedTransport?", size);
            close();
            return;
          }
          readWant_ = size;

          // Now we know the frame size, set everything up for reading the
          // payload.
          transition();
          return;

        case SocketState.RECV:
          // If we already got all the data, we should be in the SEND state.
          assert(readBufferPos_ < readWant_);

          size_t bytesRead;
          try {
            // Read as much as possible from the socket.
            bytesRead = socket_.read(readBuffer_[readBufferPos_ .. readWant_]);
          } catch (TTransportException te) {
            stderr.writefln("Connection.workSocket(): %s", te);
            close();

            return;
          }

          if (bytesRead == 0) {
            // We were notified, but no bytes could be read -> the client
            // disconnected.
            close();
            return;
          }

          readBufferPos_ += bytesRead;
          assert(readBufferPos_ <= readWant_);

          if (readBufferPos_ == readWant_) {
            // The payload has been read completely, move on.
            transition();
          }

          return;
        case SocketState.SEND:
          assert(writeBufferPos_ <= writeBuffer_.length);

          if (writeBufferPos_ == writeBuffer_.length) {
            // Nothing left to send – this shouldn't happen, just move on.
            stderr.writeln("Connection.workSocket(): WARNING: In send state, " ~
              "but no data to send.\n");
            transition();
            return;
          }

          size_t bytesSent;
          try {
            bytesSent = socket_.writePart(writeBuffer_[writeBufferPos_ .. $]);
          } catch (TTransportException te) {
            stderr.writefln("Connection.workSocket(): %s ", te);
            close();
            return;
          }

          writeBufferPos_ += bytesSent;
          assert(writeBufferPos_ <= writeBuffer_.length);

          if (writeBufferPos_ == writeBuffer_.length) {
            // The whole response has been written out, we are done.
            transition();
          }

          return;
      }
    }

    /**
     * Registers the libevent event with the passed flags, unregistering the
     * previous one (if any).
     */
    void registerEvent(short eventFlags) {
      if (eventFlags_ == eventFlags) {
        // Nothing to do if flags are the same.
        return;
      }

      // Delete the previously existing event.
      unregisterEvent();

      eventFlags_ = eventFlags;

      if (eventFlags == 0) return;

      event_set(&event_, socket_.socketHandle, eventFlags_,
        &workSocketCallback, cast(void*)this);
      event_base_set(server_.eventBase_, &event_);

      // Add the event
      if (event_add(&event_, null) == -1) {
        stderr.writeln("Connection.registerEvent(): could not event_add.");
      }
    }

    /// Ditto
    void registerReadEvent() {
      registerEvent(EV_READ | EV_PERSIST);
    }

    /// Ditto
    void registerWriteEvent() {
      registerEvent(EV_WRITE | EV_PERSIST);
    }

    /**
     * Unregisters the current libevent event, if any.
     */
    void unregisterEvent() {
      if (eventFlags_ != 0) {
        eventFlags_ = 0;
        if (event_del(&event_) == -1) {
          stderr.writeln("Connection.unregisterEvent(): event_del failed.");
          return;
        }
      }
    }

    /**
     * Closes this connection and returns it back to the server.
     */
    void close() {
      // Delete the registered libevent
      if (event_del(&event_) == -1) {
        stderr.writeln("Connection.close(): event_del failed.");
      }

      if (server_.eventHandler) {
        server_.eventHandler.deleteContext(
          connectionContext_, inputProtocol_, outputProtocol_);
      }

      // Close the socket
      socket_.close();

      // close any factory produced transports.
      factoryInputTransport_.close();
      factoryOutputTransport_.close();

      // This connection object can now be reused.
      server_.disposeConnection(this);
    }

    /// The server this connection belongs to.
    TNonblockingServer server_;

    /// The socket managed by this connection.
    TSocket socket_;

    /// The libevent object used for registering the workSocketCallback.
    event event_;

    /// Libevent flags
    short eventFlags_;

    /// Socket mode
    SocketState socketState_;

    /// Application state
    ConnectionState connState_;

    /// The size of the frame to read. If still in READ_FRAME_SIZE state, some
    /// of the bytes might not have been written, and the value might still be
    /// in network byte order. An int (not a size_t) because the frame size on
    /// the wire is specified as one.
    int readWant_;

    /// The position in the read buffer, i.e. the number of payload bytes
    /// already received from the socket in READ_REQUEST state, resp. the
    /// number of size bytes in READ_FRAME_SIZE state.
    uint readBufferPos_;

    /// Read buffer
    ubyte* readBuffer_;

    /// Read buffer size
    size_t readBufferSize_;

    /// Write buffer
    ubyte[] writeBuffer_;

    /// How far through writing are we?
    size_t writeBufferPos_;

    /// Largest size of write buffer seen since buffer was constructed
    size_t largestWriteBufferSize_;

    /// Number of calls since the last time checkIdleBufferLimit has been
    /// invoked (see TServer.resizeBufferEveryN).
    uint callsSinceResize_;

    /// Base transports the processor reads from/writes to.
    TInputRangeTransport!(ubyte[]) inputTransport_;
    TMemoryBuffer outputTransport_;

    /// The actual transports passed to the processor obtained via the
    /// transport factory.
    TTransport factoryInputTransport_;
    TTransport factoryOutputTransport_; /// Ditto

    /// Input/output protocols, connected to factory{Input, Output}Transport.
    TProtocol inputProtocol_;
    TProtocol outputProtocol_; /// Ditto.

    /// Connection context optionally created by the server event handler.
    Variant connectionContext_;
  }
}

/*
 * The request processing function, which invokes the processor for the server
 * for all the RPC messages received over a connection.
 *
 * Must be public because it is passed as alias to std.paralellism.task.
 */
void processRequest(Connection connection) {
  try {
    while (true) {
      with (connection) {
        if (server_.eventHandler) {
          server_.eventHandler.preProcess(connectionContext_, socket_);
        }

        if (!server_.processor.process(inputProtocol_, outputProtocol_,
          connectionContext_) || !inputProtocol_.transport.peek()
        ) {
          // Something went fundamentlly wrong or there is nothing more to
          // process, close the connection.
          break;
        }
      }
    }
  } catch (TTransportException ttx) {
    stderr.writefln("TNonblockingServer: Client died: %s", ttx);
  } catch (Exception e) {
    stderr.writefln("TNonblockingServer: Uncaught exception: %s", e);
  }

  connection.notifyServer();
}

private {
  version (Win32) {
    alias WSAGetLastError getSocketErrno;
  } else {
    alias getErrno getSocketErrno;
  }

  string socketErrnoString(uint errno) {
    version (Win32) {
      return sysErrorString(errno);
    } else {
      return to!string(strerror(errno));
    }
  }
}
