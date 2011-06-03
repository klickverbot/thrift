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

import core.thread : dur, Thread;
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
  this(ushort port, int sendTimeout = 0, int recvTimeout = 0) {
    port_ = port;
    sendTimeout_ = sendTimeout;
    recvTimeout_ = recvTimeout;
  }

  void sendTimeout(int sendTimeout) @property {
    sendTimeout_ = sendTimeout;
  }

  void recvTimeout(int recvTimeout) @property {
    recvTimeout_ = recvTimeout;
  }

  void retryLimit(int retryLimit) @property {
    retryLimit_ = retryLimit;
  }

  void retryDelay(int retryDelay) @property {
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
      throw new TTransportException("Could create interrupt socket pair.",
        TTransportException.Type.NOT_OPEN, __FILE__, __LINE__, e);
    }

    // TODO: Catch any SocketExceptions and rethrow them as TTransportException.
    serverSocket_ = new Socket(AddressFamily.INET, SocketType.STREAM,
      ProtocolType.TCP);

    alias SocketOptionLevel.SOCKET lvlSock;

    // Set reusaddress to prevent 2MSL delay on accept.
    serverSocket_.setOption(lvlSock, SocketOption.REUSEADDR, true);

    // Set TCP buffer sizes.
    if (tcpSendBuffer_ > 0) {
      serverSocket_.setOption(lvlSock, SocketOption.SNDBUF, tcpSendBuffer_);
    }
    if (tcpRecvBuffer_ > 0) {
      serverSocket_.setOption(lvlSock, SocketOption.RCVBUF, tcpRecvBuffer_);
    }

    // Turn linger off, don't want to block on calls to close.
    serverSocket_.setOption(lvlSock, SocketOption.LINGER, linger(0, 0));

    // If we are working with a TCP socket, set TCP_NODELAY.
    serverSocket_.setOption(lvlSock, SocketOption.TCP_NODELAY, true);

    auto localAddr = new InternetAddress("0.0.0.0", port_);

    int retries;
    while (true) {
      try {
        serverSocket_.bind(localAddr);
        break;
      } catch (SocketException) {}
      retries++;
      if (retries < retryLimit_) {
        Thread.sleep(dur!"seconds"(retryDelay_));
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
        // Check for an interrupt message on the interrupt socket..
        if (set.isSet(intRecvSocket_)) {
          ubyte[1] buf;
          try {
            auto result = intRecvSocket_.receive(buf);
            if (result == Socket.ERROR) {
              stderr.writeln("TServerSocket.acceptImpl(): Error receiving" ~
                " interrupt message: %s", to!string(strerror(errno)));
            }
          } catch (SocketException e) {
            stderr.writeln("TServerSocket.acceptImpl(): Error receiving" ~
              " interrupt message: %s", to!string(e));
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
      return client;
    } catch (SocketException e) {
      throw new TTransportException(TTransportException.Type.UNKNOWN,
        __FILE__, __LINE__, e);
    }
  }

private:
  ushort port_;
  int acceptBacklog_ = 1024;
  int sendTimeout_;
  int recvTimeout_;
  int retryLimit_;
  int retryDelay_;
  int tcpSendBuffer_;
  int tcpRecvBuffer_;

  Socket serverSocket_;
  Socket intRecvSocket_;
  Socket intSendSocket_;
}

version (unittest) {
  import std.concurrency : spawn;
}

unittest {
  // Test interrupt().
  auto sock = new TServerSocket(0);
  sock.listen();

  static void interruptSocket(shared(TServerSocket) socket) {
    // Sleep for a bit until the socket is accepting.
    Thread.sleep(dur!"msecs"(1));
    (cast(TServerSocket) socket).interrupt();
  }
  spawn(&interruptSocket, cast(shared(TServerSocket))sock);

  try {
    sock.accept();
    throw new Exception("Didn't interrupt, test failed.");
  } catch (TTransportException e) {
    if (e.type != TTransportException.Type.INTERRUPTED) throw e;
  }
}
