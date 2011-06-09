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
module thrift.transport.serversocket;

import core.thread : dur, Duration, Thread;
import core.stdc.errno : errno, EINTR;
import core.stdc.string : strerror;
import std.array : empty;
import std.exception : enforce;
import std.stdio : stderr; // No proper logging support yet.
import std.socket;
import thrift.transport.base;
import thrift.transport.server;
import thrift.transport.socket;

/**
 * Server socket implementation of TServerTransport.
 *
 * Maps to std.socket listen()/accept(); only provides TCP/IP sockets (i.e. no
 * Unix sockets) for now, because they are not supported in std.socket; IPv4
 * only for the same reason.
 */
class TServerSocket : TServerTransport {
  this(ushort port, Duration sendTimeout = dur!"hnsecs"(0),
    Duration recvTimeout = dur!"hnsecs"(0))
  {
    port_ = port;
    sendTimeout_ = sendTimeout;
    recvTimeout_ = recvTimeout;
  }

  void sendTimeout(Duration sendTimeout) @property {
    sendTimeout_ = sendTimeout;
  }

  void recvTimeout(Duration recvTimeout) @property {
    recvTimeout_ = recvTimeout;
  }

  void retryLimit(int retryLimit) @property {
    retryLimit_ = retryLimit;
  }

  void retryDelay(Duration retryDelay) @property {
    retryDelay_ = retryDelay;
  }

  void tcpSendBuffer(int tcpSendBuffer) @property {
    tcpSendBuffer_ = tcpSendBuffer;
  }

  void tcpRecvBuffer(int tcpRecvBuffer) @property {
    tcpRecvBuffer_ = tcpRecvBuffer;
  }

  override void listen() {
    try {
      auto pair = socketPair();
      intSendSocket_ = pair[0];
      intRecvSocket_ = pair[1];
    } catch (SocketException e) {
      throw new TTransportException("Could not create interrupt socket pair: " ~
        to!string(e), TTransportException.Type.NOT_OPEN);
    }

    try {
      serverSocket_ = new Socket(AddressFamily.INET, SocketType.STREAM,
        ProtocolType.TCP);
    } catch (SocketException e) {
      throw new TTransportException("Could not create accepting socket: " ~
        to!string(e), TTransportException.Type.NOT_OPEN);
    }

    alias SocketOptionLevel.SOCKET lvlSock;

    // Set reusaddress to prevent 2MSL delay on accept.
    try {
      serverSocket_.setOption(lvlSock, SocketOption.REUSEADDR, true);
    } catch (SocketException e) {
      throw new TTransportException("Could not set REUSEADDR socket option: " ~
        to!string(e), TTransportException.Type.NOT_OPEN);
    }

    // Set TCP buffer sizes.
    if (tcpSendBuffer_ > 0) {
      try {
        serverSocket_.setOption(lvlSock, SocketOption.SNDBUF, tcpSendBuffer_);
      } catch (SocketException e) {
        throw new TTransportException("Could not set socket send buffer size: " ~
          to!string(e), TTransportException.Type.NOT_OPEN);
      }
    }

    if (tcpRecvBuffer_ > 0) {
      try {
        serverSocket_.setOption(lvlSock, SocketOption.RCVBUF, tcpRecvBuffer_);
      } catch (SocketException e) {
        throw new TTransportException("Could not set receive send buffer size: " ~
          to!string(e), TTransportException.Type.NOT_OPEN);
      }
    }

    // Turn linger off, don't want to block on calls to close.
    try {
      serverSocket_.setOption(lvlSock, SocketOption.LINGER, linger(0, 0));
    } catch (SocketException e) {
      throw new TTransportException("Could not disable socket linger: " ~
        to!string(e), TTransportException.Type.NOT_OPEN);
    }

    // Set TCP_NODELAY. Do not fail hard as root privileges might be required
    // on Linux to set the option.
    try {
      serverSocket_.setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY,
        true);
    } catch (SocketException e) {
      throw new TTransportException("Could not disable Nagle's algorithm: " ~
        to!string(e), TTransportException.Type.NOT_OPEN);
    }

    auto localAddr = new InternetAddress("0.0.0.0", port_);

    int retries;
    while (true) {
      try {
        serverSocket_.bind(localAddr);
        break;
      } catch (SocketException) {}
      retries++;
      if (retries < retryLimit_) {
        Thread.sleep(retryDelay_);
      } else {
        throw new TTransportException("Could not bind.",
          TTransportException.Type.NOT_OPEN);
      }
    }

    serverSocket_.listen(acceptBacklog_);
  }

  override void close() {
    assert(serverSocket_, "Called close() on non-listening TServerSocket.");
    serverSocket_.shutdown(SocketShutdown.BOTH);
    serverSocket_.close();
    serverSocket_ = null;

    intSendSocket_.close();
    intSendSocket_ = null;

    intRecvSocket_.close();
    intRecvSocket_ = null;
  }

  override void interrupt() {
    assert(intSendSocket_, "Called interrupt() on non-listening TServerSocket.");
    // Just ping the interrupt socket to throw acceptImpl() out of the
    // select() call.
    intSendSocket_.send(cast(void[])[0]);
  }

protected:
  override TTransport acceptImpl() {
    assert(serverSocket_, "Called accept() on non-listening TServerSocket.");

    enum maxEintrs = 5;
    uint numEintrs = 0;

    while (true) {
      auto set = new SocketSet(2);
      set.add(serverSocket_);
      set.add(intRecvSocket_);

      auto ret = Socket.select(set, null, null);
      enforce(ret != 0, new TTransportException("Socket.select() returned 0.",
        TTransportException.Type.UNKNOWN));

      if (ret < 0) {
        // error cases
        if (errno == EINTR && (numEintrs++ < maxEintrs)) {
          // EINTR needs to be handled manually and we can tolerate
          // a certain number
          continue;
        }
        throw new TTransportException("Unknown error on Socket.select()",
          TTransportException.Type.UNKNOWN, errno);
      } else {
        // Check for a ping on the interrupt socket.
        if (set.isSet(intRecvSocket_)) {
          ubyte[1] buf;
          try {
            auto result = intRecvSocket_.receive(buf);
            if (result == Socket.ERROR) {
              stderr.writefln("TServerSocket.acceptImpl(): Error receiving" ~
                " interrupt message: %s", strerror(errno));
            }
          } catch (SocketException e) {
            stderr.writefln("TServerSocket.acceptImpl(): Error receiving" ~
              " interrupt message: %s", e);
          }
          throw new TTransportException(TTransportException.Type.INTERRUPTED);
        }

        // Check for the actual server socket having a connection waiting.
        if (set.isSet(serverSocket_)) {
          break;
        }
      }
    }

    try {
      auto client = new TSocket(serverSocket_.accept());
      client.sendTimeout = sendTimeout_;
      client.recvTimeout = recvTimeout_;
      return client;
    } catch (SocketException e) {
      throw new TTransportException("Unknown error on accepting: " ~
        to!string(e), TTransportException.Type.UNKNOWN);
    }
  }

private:
  ushort port_;
  int acceptBacklog_ = 1024;
  Duration sendTimeout_;
  Duration recvTimeout_;
  int retryLimit_;
  Duration retryDelay_;
  int tcpSendBuffer_;
  int tcpRecvBuffer_;

  Socket serverSocket_;
  Socket intRecvSocket_;
  Socket intSendSocket_;
}

unittest {
  // Test interrupt().
  {
    auto sock = new TServerSocket(0);
    sock.listen();
    scope (exit) sock.close();

    auto intThread = new Thread({
      // Sleep for a bit until the socket is accepting.
      Thread.sleep(dur!"msecs"(1));
      sock.interrupt();
    });
    intThread.start();

    try {
      sock.accept();
      throw new Exception("Didn't interrupt, test failed.");
    } catch (TTransportException e) {
      if (e.type != TTransportException.Type.INTERRUPTED) throw e;
    }
  }

  // Test receive() timeout on accepted client sockets.
  {
    immutable port = 11122;
    auto timeout = dur!"msecs"(500);
    auto serverSock = new TServerSocket(port, timeout, timeout);
    serverSock.listen();
    scope (exit) serverSock.close();

    auto clientSock = new TSocket("127.0.0.1", port);
    clientSock.open();
    scope (exit) clientSock.close();

    shared bool hasTimedOut;
    auto recvThread = new Thread({
      auto sock = serverSock.accept();
      ubyte[1] data;
      try {
        sock.read(data);
      } catch (TTransportException e) {
        if (e.type == TTransportException.Type.TIMED_OUT) {
          hasTimedOut = true;
        } else {
          stderr.writeln(e);
        }
      }
    });
    recvThread.isDaemon = true;
    recvThread.start();

    // Wait for the timeout, with a little bit of spare time.
    Thread.sleep(timeout + dur!"msecs"(50));
    enforce(hasTimedOut,
      "Client socket receive() blocked for longer than recvTimeout.");
  }
}
