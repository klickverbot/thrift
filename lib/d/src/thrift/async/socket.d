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
module thrift.async.socket;

import core.stdc.errno : EPIPE, ENOTCONN;
import core.thread : Fiber;
import core.time : Duration;
import std.array : empty;
import std.conv : to;
import std.exception : enforce;
import std.socket;
import std.stdio : stderr; // No proper logging support yet.
import thrift.async.base;
import thrift.transport.base;
import thrift.transport.socket : TSocketBase;
import thrift.util.endian;
import thrift.util.socket;

version (Windows) {
  import std.c.windows.winsock : connect, sockaddr, sockaddr_in;
} else version (Posix) {
  import core.sys.posix.netinet.in_ : sockaddr_in;
  import core.sys.posix.sys.socket : connect, sockaddr;
} else static assert(0, "Don't know connect/sockaddr_in on this platform.");

/**
 * Non-blocking socket implementation of the TTransport interface.
 *
 * Whenever a socket operation would block, TAsyncSocket registers a callback
 * with the specified TAsyncSocketManager and yields.
 *
 * As for thrift.transport.socket, due to the limitations of std.socket, only
 * TCP/IPv4 sockets (i.e. no Unix sockets or IPv6) are currently supported.
 */
class TAsyncSocket : TSocketBase, TAsyncTransport {
  /**
   * Constructor that takes an already created, connected (!) socket.
   *
   * Params:
   *   asyncManager = The TAsyncSocketManager to use for non-blocking I/O.
   *   socket = Already created, connected socket object. Will be switched to
   *     non-blocking mode if it isn't already.
   */
  this(TAsyncSocketManager asyncManager, Socket socket) {
    asyncManager_ = asyncManager;
    socket.blocking = false;
    super(socket);
  }

  /**
   * Creates a new unconnected socket that will connect to the given host
   * on the given port.
   *
   * Params:
   *   asyncManager = The TAsyncSocketManager to use for non-blocking I/O.
   *   host = Remote host.
   *   port = Remote port.
   */
  this(TAsyncSocketManager asyncManager, string host, ushort port) {
    asyncManager_ = asyncManager;
    super(host, port);
  }

  override TAsyncManager asyncManager() @property {
    return asyncManager_;
  }

  /**
   * Asynchronously connects the socket.
   *
   * Completes without blocking and defers further operations on the socket
   * until the connection is established. If connecting fails, this is
   * currently not indicated in any way other than every call to read/write
   * failing.
   */
  override void open() {
    if (isOpen) return;

    enforce(!host_.empty, new TTransportException(
      TTransportException.Type.NOT_OPEN, "Cannot open null host."));
    enforce(port_ != 0, new TTransportException(
      TTransportException.Type.NOT_OPEN, "Cannot open with null port."));

    socket_ = new TcpSocket(AddressFamily.INET);
    socket_.blocking = false;
    setSocketOpts();

    // Cannot use std.socket.Socket.connect here because it hides away
    // EINPROGRESS/WSAWOULDBLOCK, and cannot use InternetAddress here because
    // it does not provide access to the sockaddr_in struct outside std.socket.
    auto addr = InternetAddress.parse(host_);
    if (addr == InternetAddress.ADDR_NONE) {
      auto host = new InternetHost;
      if (!host.getHostByName(host_)) {
        throw new TTransportException(`Unable to resolve host "` ~ host_ ~ `".`,
          TTransportException.Type.NOT_OPEN);
      }
      addr = host.addrList[0];
    }

    sockaddr_in sin;
    sin.sin_family = AddressFamily.INET;
    sin.sin_addr.s_addr = hostToNet(addr);
    sin.sin_port = hostToNet(port);

    auto errorCode = connect(socket_.handle, cast(sockaddr*)&sin, sin.sizeof);
    if (errorCode == 0) {
      // If the connection could be established immediately, just return. I
      // don't know if this ever happens.
      return;
    }

    auto errno = getSocketErrno();
    if (errno != CONNECT_INPROGRESS_ERRNO) {
      throw new TTransportException(`Could not establish connection to "` ~
        host_ ~ `": ` ~ socketErrnoString(errno),
        TTransportException.Type.NOT_OPEN);
    }

    // This is the expected case: connect() signalled that the connection
    // is being established in the background. Queue up a work item with the
    // async manager which just defers any other operations on this
    // TAsyncSocket instance until the socket is ready.
    asyncManager_.execute(TAsyncWorkItem(this, {
      auto fiber = Fiber.getThis();
      asyncManager_.addOneshotListener(socket_, TAsyncEventType.WRITE,
        (TAsyncEventReason reason){ fiber.call(); });
      Fiber.yield();

      int errorFlag;
      socket_.getOption(SocketOptionLevel.SOCKET, cast(SocketOption)SO_ERROR,
        errorFlag);

      if (errorFlag) {
        // Close the connection, so that subsequent work items fail immediately.
        close();
      }
    }));
  }

  /**
   * Closes the socket.
   *
   * Note: Currently, calling this while there are still pending asynchronous
   *   operations for this connection yields undefined behavior.
   */
  override void close() {
    if (!isOpen) return;

    socket_.close();
    socket_ = null;
  }

  override bool peek() {
    if (!isOpen) return false;

    ubyte buf;
    auto r = socket_.receive((&buf)[0..1], SocketFlags.PEEK);
    if (r == Socket.ERROR) {
      auto lastErrno = getSocketErrno();
      static if (connresetOnPeerShutdown) {
        if (lastErrno == ECONNRESET) {
          close();
          return false;
        }
      }
      throw new TTransportException("Peeking into socket failed: " ~
        socketErrnoString(lastErrno), TTransportException.Type.UNKNOWN);
    }
    return (r > 0);
  }

  override size_t read(ubyte[] buf) {
    enforce(isOpen, new TTransportException(
      "Cannot read if socket is not open.", TTransportException.Type.NOT_OPEN));

    typeof(getSocketErrno()) lastErrno;

    auto r = yieldOnEagain(socket_.receive(cast(void[])buf),
      TAsyncEventType.READ);

    // If recv went fine, immediately return.
    if (r >= 0) return r;

    // Something went wrong, find out how to handle it.
    lastErrno = getSocketErrno();

    static if (connresetOnPeerShutdown) {
      // See top comment.
      if (lastErrno == ECONNRESET) {
        return 0;
      }
    }

    throw new TTransportException("Receiving from socket failed: " ~
      socketErrnoString(lastErrno), TTransportException.Type.UNKNOWN);
  }

  override void write(in ubyte[] buf) {
    size_t sent;
    while (sent < buf.length) {
      sent += writeSome(buf[sent .. $]);
    }
    assert(sent == buf.length);
  }

  override size_t writeSome(in ubyte[] buf) {
    enforce(isOpen, new TTransportException(
      "Cannot write if socket is not open.", TTransportException.Type.NOT_OPEN));

    auto r = yieldOnEagain(socket_.send(buf), TAsyncEventType.WRITE);

    // Everything went well, just return the number of bytes written.
    if (r > 0) return r;

    // Handle error conditions. TODO: Windows.
    if (r < 0) {
      auto lastErrno = getSocketErrno();

      auto type = TTransportException.Type.UNKNOWN;
      if (lastErrno == EPIPE || lastErrno == ECONNRESET || lastErrno == ENOTCONN) {
        type = TTransportException.Type.NOT_OPEN;
        close();
      }

      throw new TTransportException("Sending to socket failed: " ~
        socketErrnoString(lastErrno), type);
    }

    // send() should never return 0.
    throw new TTransportException("Sending to socket failed (0 bytes written).",
      TTransportException.Type.UNKNOWN);
  }

private:
  T yieldOnEagain(T)(lazy T call, TAsyncEventType eventType) {
    while (true) {
      auto result = call();
      if (result != Socket.ERROR || getSocketErrno() != EAGAIN) return result;

      // We got an EAGAIN result, register a callback to return here once some
      // event happens and yield.
      // TODO: It could be that we are needlessly capturing context here,
      // maybe use scoped delegate?

      Duration timeout = void;
      final switch (eventType) {
        case TAsyncEventType.READ:
          timeout = recvTimeout_;
          break;
        case TAsyncEventType.WRITE:
          timeout = sendTimeout_;
          break;
      }

      auto fiber = Fiber.getThis();
      TAsyncEventReason eventReason = void;
      asyncManager_.addOneshotListener(socket_, eventType, timeout,
        (TAsyncEventReason reason) {
          eventReason = reason;
          fiber.call();
        }
      );

      Fiber.yield();

      if (eventReason == TAsyncEventReason.TIMED_OUT) {
        // If we are cancelling the request due to a timed out operation, the
        // connection is in an undefined state, because the server could decide
        // to send the requested data later, or we could have already been half-
        // way into writing a request. Thus, we close the connection to make any
        // possibly queued up work items fail immediately. Besides, the server
        // is unlikely to immediately recover after a socket-level timeout has
        // experienced anyway.
        close();

        throw new TTransportException(
          "Timed out while waiting for socket to get ready for " ~
          to!string(eventType) ~ ".", TTransportException.Type.TIMED_OUT);
      }
    }
  }

  /// The TAsyncSocketManager to use for non-blocking I/O.
  TAsyncSocketManager asyncManager_;
}

private {
  // std.socket doesn't include SO_ERROR for reasons unknown.
  version (linux) {
    enum SO_ERROR = 4;
  } else version (OSX) {
    enum SO_ERROR = 0x1007;
  } else version (FreeBSD) {
    enum SO_ERROR = 0x1007;
  } else {
    // TODO: Windows.
    static assert(0, "Don't know SO_ERROR on this platform.");
  }
}
