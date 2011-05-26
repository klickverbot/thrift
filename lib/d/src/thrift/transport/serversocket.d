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
import std.array : empty;
import std.exception : enforce;
import std.stdio : stderr, writeln, writefln;
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

    // Set the (accepting) socket to NONBLOCK.
    // TODO: Enable this once interruption sockets are implemented for D.
    // serverSocket_.blocking = false;

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
    serverSocket_.shutdown(SocketShutdown.BOTH);
    serverSocket_.close();
  }

  override void interrupt() {
    throw new Exception("TServerSocket.interrupt() not implemented yet for D.");
  }

protected:
  override TTransport acceptImpl() {
    auto client = new TSocket(serverSocket_.accept());
    return client;
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
}
