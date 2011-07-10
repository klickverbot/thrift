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

import core.thread : Thread;
import core.time : Duration;
import std.array : empty;
import std.conv : to;
import std.exception : enforce;
import std.socket;
import std.stdio : stderr; // No proper logging support yet.
import thrift.transport.base;

private {
  // FreeBSD and OS X return -1 and set ECONNRESET if socket was closed by
  // the other side, we need to check for that before throwing an exception.
  version (FreeBSD) {
    enum connresetOnPeerShutdown = true;
  } else version (OSX) {
    enum connresetOnPeerShutdown = true;
  } else {
    enum connresetOnPeerShutdown = false;
  }

  version (Win32) {
    import std.c.windows.winsock : WSAGetLastError, WSAEINTR;
    import std.windows.syserror : sysErrorString;
  } else {
    import core.stdc.errno : getErrno, EAGAIN, ECONNRESET, EINTR, EWOULDBLOCK;
    import core.stdc.string : strerror;
  }

  version (Win32) {
    alias WSAGetLastError getSocketErrno;
    enum INTERRUPTED_ERRNO = WSAEINTR;
    // See http://msdn.microsoft.com/en-us/library/ms740668.aspx.
    enum TIMEOUT_ERRNO = 10060;
  } else {
    alias getErrno getSocketErrno;
    alias EINTR INTERRUPTED_ERRNO;

    // TODO: The C++ TSocket implementation mentions that EAGAIN can also be
    // set (undocumentedly) in out of ressource conditions; adapt the code
    // accordingly.
    alias EAGAIN TIMEOUT_ERRNO;
  }

  string socketErrnoString(uint errno) {
    version (Win32) {
      return sysErrorString(errno);
    } else {
      return to!string(strerror(errno));
    }
  }
}

/**
 * Socket implementation of the TTransport interface.
 *
 * Due to the limitations of std.socket, only TCP/IPv4 sockets (i.e. no Unix
 * sockets or IPv6) are currently supported.
 */
class TSocket : TBaseTransport {
  /**
   * Constructor that takes an already created, connected (!) socket.
   *
   * Params:
   *   socket = Already created, connected socket object.
   */
  this(Socket socket) {
    socket_ = socket;
    setSocketOpts();
    maxRecvRetries = DEFAULT_MAX_RECV_RETRIES;
  }

  /**
   * Creates a new unconnected socket that will connect to the given host
   * on the given port.
   *
   * Params:
   *   host = Remote host
   *   port = Remote port
   */
  this(string host, ushort port) {
    host_ = host;
    port_ = port;
    maxRecvRetries = DEFAULT_MAX_RECV_RETRIES;
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
      socket_ = new TcpSocket(AddressFamily.INET);
      setSocketOpts();
    }

    try {
      socket_.connect(new InternetAddress(host_, port_));
    } catch (SocketException e) {
      throw new TTransportException(TTransportException.Type.NOT_OPEN,
        __FILE__, __LINE__, e);
    }
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
    typeof(getSocketErrno()) lastErrno;
    ushort tries;
    while (tries++ <= maxRecvRetries_) {
      auto r = socket_.receive(cast(void[])buf);

      // If recv went fine, immediately return.
      if (r >= 0) return r;

      // Something went wrong, find out how to handle it.
      lastErrno = getSocketErrno();

      // TODO: Handle EAGAIN like C++ does.

      if (lastErrno == INTERRUPTED_ERRNO) {
        // If the syscall was interrupted, just try again.
        continue;
      }

      static if (connresetOnPeerShutdown) {
        // See top comment.
        if (lastErrno == ECONNRESET) {
          return 0;
        }
      }

      // Not an error which is handled in a special way, just leave the loop.
      break;
    }

    if (lastErrno == TIMEOUT_ERRNO) {
      throw new TTransportException(TTransportException.Type.TIMED_OUT);
    } else {
      throw new TTransportException("Receiving from socket failed: " ~
        socketErrnoString(lastErrno), TTransportException.Type.UNKNOWN);
    }
  }

  override void write(in ubyte[] buf) {
    size_t sent;
    while (sent < buf.length) {
      auto b = writePart(buf[sent .. $]);
      if (b == 0) {
        // Couldn't send due to lack of system resources, wait a bit and try
        // again.
        Thread.sleep(dur!"usecs"(50));
      }
      sent += b;
    }
    assert(sent == buf.length);
  }

  /**
   * Writes as much data to the socket as there can be in a single OS call.
   *
   * Params:
   *   buf = Data to write.
   *
   * Returns: The actual number of bytes written. Never more than buf.length.
   */
  size_t writePart(in ubyte[] buf) out (written) {
    assert(written <= buf.length, "More data written than tried to?!");
  } body {
    assert(isOpen, "Called writePart() on non-open socket!");

    auto r = socket_.send(buf);
    if (r < 0) {
      auto lastErrno = getSocketErrno();

      // TODO: Windows.
      if (lastErrno == EWOULDBLOCK || lastErrno == EAGAIN) {
        // Not an exceptional error per se – even with blocking sockets,
        // EAGAIN apparently is returned sometimes on out-of-resource
        // conditions (see the C++ implementation for details). Also, this
        // allows using TSocket with non-blocking sockets e.g. in
        // TNonblockingServer.
        return 0;
      }

      throw new TTransportException("Receiving from socket failed: " ~
        socketErrnoString(lastErrno), TTransportException.Type.UNKNOWN);
    }
    return r;
  }

  /**
   * Returns the host name of the peer.
   *
   * The socket must be open when calling this.
   */
  string getPeerHost() {
    enforce(isOpen, new TTransportException("Cannot get peer host for " ~
      "closed socket.", TTransportException.Type.NOT_OPEN));

    if (!peerHost_) {
      peerHost_ = peerAddress.toHostNameString();
    }

    return peerHost_;
  }

  /**
   * Returns the port of the peer.
   *
   * The socket must be open when calling this.
   */
  ushort getPeerPort() {
    enforce(isOpen, new TTransportException("Cannot get peer port for " ~
      "closed socket.", TTransportException.Type.NOT_OPEN));

    if (!peerPort_) {
      peerPort_ = peerAddress.port();
    }

    return peerPort_;
  }

  /// The host to connect to.
  string host() @property {
    return host_;
  }

  /// The port to connect to.
  ushort port() @property {
    return port_;
  }

  /// The socket send timeout.
  Duration sendTimeout() const @property {
    return sendTimeout_;
  }

  /// Ditto
  void sendTimeout(Duration value) @property {
    sendTimeout_ = value;
    setTimeout(SocketOption.SNDTIMEO, value);
  }

  /// The socket receiving timeout. Values smaller than 500 ms are not
  /// supported on Windows.
  Duration recvTimeout() const @property {
    return recvTimeout_;
  }

  /// Ditto
  void recvTimeout(Duration value) @property {
    recvTimeout_ = value;
    setTimeout(SocketOption.RCVTIMEO, value);
  }

  /**
   * Maximum number of retries for receiving from socket on read() in case of
   * EAGAIN/EINTR.
   */
  ushort maxRecvRetries() @property const {
    return maxRecvRetries_;
  }

  /// Ditto
  void maxRecvRetries(ushort value) @property {
    maxRecvRetries_ = value;
  }

  /// Ditto
  enum DEFAULT_MAX_RECV_RETRIES = 5;

  /**
   * Returns the OS handle of the underlying socket.
   *
   * Should not usually be used directly, but access to it can be necessary
   * to interface with C libraries.
   */
  typeof(socket_.handle()) socketHandle() @property {
    return socket_.handle();
  }

protected:
  InternetAddress peerAddress() @property {
    if (!peerAddress_) {
      peerAddress_ = cast(InternetAddress) socket_.remoteAddress();
      assert(peerAddress_);
    }
    return peerAddress_;
  }

private:
  /**
   * Sets the needed socket options.
   */
  void setSocketOpts() {
    try {
      alias SocketOptionLevel.SOCKET lvlSock;
      socket_.setOption(lvlSock, SocketOption.LINGER, linger(0, 0));
      socket_.setOption(lvlSock, SocketOption.SNDTIMEO, sendTimeout_);
      socket_.setOption(lvlSock, SocketOption.RCVTIMEO, recvTimeout_);
    } catch (SocketException e) {
      stderr.writefln("Could not set socket option: %s", e);
    }

    // Just try to disable Nagle's algorithm – this will fail if we are passed
    // in a non-TCP socket via the Socket-accepting constructor.
    try {
      socket_.setOption(SocketOptionLevel.TCP, SocketOption.TCP_NODELAY, true);
    } catch (SocketException e) {}
  }

  void setTimeout(SocketOption type, Duration value) {
    assert(type == SocketOption.SNDTIMEO || type == SocketOption.RCVTIMEO);
    version (Win32) {
      if (value > dur!"hnsecs"(0) && value < dur!"msecs"(500)) {
        stderr.writefln(
          "Socket %s timeout of %s ms might be raised to 500 ms on Windows.",
          (type == SocketOption.SNDTIMEO) ? "send" : "receive",
          value.total!"msecs"
        );
      }
    }

    if (socket_) {
      try {
        socket_.setOption(SocketOptionLevel.SOCKET, type, value);
      } catch (SocketException e) {
        throw new TTransportException(
          "Could not set send timeout: " ~ socketErrnoString(e.errorCode),
          TTransportException.Type.UNKNOWN,
          __FILE__,
          __LINE__
        );
      }
    }
  }

  /// Remote host.
  string host_;

  /// Remote port.
  ushort port_;

  /// Timeout for sending.
  Duration sendTimeout_;

  /// Timeout for receiving.
  Duration recvTimeout_;

  /// Maximum number of receive() retries.
  ushort maxRecvRetries_;

  /// Cached peer address.
  InternetAddress peerAddress_;

  /// Cached peer host name.
  string peerHost_;

  /// Cached peer port.
  ushort peerPort_;

  /// Wrapped socket object.
  Socket socket_;
}
