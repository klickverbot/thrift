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
module thrift.transport.socket;

import core.stdc.errno;
import std.array : empty;
import std.exception : enforce;
import std.socket;
import thrift.transport.base;

/**
 * Socket implementation of the TTransport interface. To be commented soon!
 *
 * TODO: Support timeout?
 */
public class TSocket : TTransport {
  /**
   * Constructor that takes an already created, connected (!) socket.
   *
   * @param socket Already created, connected socket object
   */
  this(Socket socket) {
    socket_ = socket;
    // Catch SocketExceptions like for Java here?
    socket_.setOption(SocketOptionLevel.SOCKET, SocketOption.LINGER, linger(0, 0));
    socket_.setOption(SocketOptionLevel.SOCKET, SocketOption.TCP_NODELAY, true);
  }

  /**
   * Creates a new unconnected socket that will connect to the given host
   * on the given port.
   *
   * @param host Remote host
   * @param port Remote port
   */
  this(string host, ushort port) {
    host_ = host;
    port_ = port;
  }

  /**
   * Checks whether the socket is connected.
   */
  override bool isOpen() @property {
    return socket_ !is null;
  }

  /**
   * Connects the socket.
   */
  override void open() {
    if (isOpen) return;

    enforce(!host_.empty, new TTransportException(
      TTransportException.Type.NOT_OPEN, "Cannot open null host."));
    enforce(port_ != 0, new TTransportException(
      TTransportException.Type.NOT_OPEN, "Cannot open with null port."));

    if (socket_ is null) {
      initSocket();
    }

    // TODO: Catch and wrap exception here?
    socket_.connect(new InternetAddress(host_, port_));
  }

  /**
   * Closes the socket.
   */
  override void close() {
    if (socket_ !is null) {
      socket_.close();
      socket_ = null;
    }
  }

  override bool peek() {
    if (!isOpen) return false;

    ubyte buf;
    auto r = socket_.receive((&buf)[0..1], SocketFlags.PEEK);
    if (r == -1) {
      version (FreeBSD) {
        if (errno == ECONNRESET) {
          close();
          return false;
        }
      }
      throw new TTransportException("Peeking into socket failed",
        TTransportException.Type.UNKNOWN, errno);
    }
    return (r > 0);
  }

  override uint read(ubyte[] buf) {
    assert(isOpen, "Called read on non-open socket.");
    auto r = socket_.receive(cast(void[])buf);
    if (r == -1) {
      version (FreeBSD) {
        if (errno == ECONNRESET) {
          close();
          return false;
        }
      }
      throw new TTransportException("Receiving from socket failed",
        TTransportException.Type.UNKNOWN, errno);
    }
    return r;
  }

  override void write(in ubyte[] buf) {
    assert(isOpen, "Called write on non-open socket.");
    auto r = socket_.send(buf);
    if (r == -1) {
      version (FreeBSD) {
        if (errno == ECONNRESET) {
          close();
          return false;
        }
      }
      throw new TTransportException("Writing to socket failed",
        TTransportException.Type.UNKNOWN, errno);
    }
  }

private:
  /**
   * Initializes the socket object.
   */
  void initSocket() {
    socket_ = new TcpSocket(AddressFamily.INET);
    // Catch SocketExceptions like for Java here?
    socket_.setOption(SocketOptionLevel.SOCKET, SocketOption.LINGER, linger(0, 0));
    socket_.setOption(SocketOptionLevel.SOCKET, SocketOption.TCP_NODELAY, true);
  }

  /// Wrapped socket object.
  Socket socket_ = null;

  /// Remote host.
  string host_  = null;

  /// Remote port.
  ushort port_ = 0;
}
