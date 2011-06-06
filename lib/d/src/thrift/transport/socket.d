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

import core.time : Duration;
import std.array : empty;
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
    import std.c.windows.winsock : WSAGetLastError;
    import std.windows.syserror : sysErrorString;
  } else {
    import core.stdc.errno : getErrno, EAGAIN, ECONNRESET;
    import core.stdc.string : strerror;
  }

  version (Win32) {
    alias WSAGetLastError getSocketErrno;
    // See http://msdn.microsoft.com/en-us/library/ms740668.aspx.
    enum TIMEOUT_ERRNO = 10060;
  } else {
    alias getErrno getSocketErrno;

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
      auto errno = getSocketErrno();
      static if (connresetOnPeerShutdown) {
        if (errno == ECONNRESET) {
          close();
          return false;
        }
      }
      throw new TTransportException("Peeking into socket failed: " ~
        socketErrnoString(errno), TTransportException.Type.UNKNOWN);
    }
    return (r > 0);
  }

  override size_t read(ubyte[] buf) {
    auto r = socket_.receive(cast(void[])buf);
    if (r == -1) {
      auto errno = getSocketErrno();
      static if (connresetOnPeerShutdown) {
        if (errno == ECONNRESET) {
          return 0;
        }
      }
      if (errno == TIMEOUT_ERRNO) {
        throw new TTransportException(TTransportException.Type.TIMED_OUT);
      } else {
        throw new TTransportException("Receiving from socket failed: " ~
          socketErrnoString(errno), TTransportException.Type.UNKNOWN);
      }
    }
    return r;
  }

  override void write(in ubyte[] buf) {
    auto r = socket_.send(buf);
    if (r == -1) {
      auto errno = getSocketErrno();
      static if (connresetOnPeerShutdown) {
        if (errno == ECONNRESET) {
          close();
          return;
        }
      }
      if (errno == TIMEOUT_ERRNO) {
        throw new TTransportException(TTransportException.Type.TIMED_OUT);
      } else {
        throw new TTransportException("Receiving from socket failed: " ~
          socketErrnoString(errno), TTransportException.Type.UNKNOWN);
      }
    }
  }

  Duration sendTimeout() const @property {
    return sendTimeout_;
  }

  void sendTimeout(Duration value) @property {
    sendTimeout_ = value;
    setTimeout(SocketOption.SNDTIMEO, value);
  }

  Duration recvTimeout() const @property {
    return recvTimeout_;
  }

  void recvTimeout(Duration value) @property {
    recvTimeout_ = value;
    setTimeout(SocketOption.RCVTIMEO, value);
  }

private:
  /**
   * Sets the needed socket options.
   */
  void setSocketOpts() {
    try {
      alias SocketOptionLevel.SOCKET lvlSock;
      socket_.setOption(lvlSock, SocketOption.LINGER, linger(0, 0));
      socket_.setOption(lvlSock, SocketOption.TCP_NODELAY, true);
      socket_.setOption(lvlSock, SocketOption.SNDTIMEO, sendTimeout_);
      socket_.setOption(lvlSock, SocketOption.RCVTIMEO, recvTimeout_);
    } catch (SocketException e) {
      stderr.writefln("Could not set socket option: %s", e);
    }
  }

  void setTimeout(SocketOption type, Duration value) {
    assert(type == SocketOption.SNDTIMEO || type == SocketOption.RCVTIMEO);
    version (Win32) {
      auto msecs = value.total!"msecs";
      if (msecs < 500) {
        stderr.writefln(
          "Socket %s timeout of %s ms might be raised to 500 ms on Windows.",
          (type == SocketOption.SNDTIMEO) ? "send" : "receive",
          msecs
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

  /// Wrapped socket object.
  Socket socket_;

  /// Remote host.
  string host_;

  /// Remote port.
  ushort port_;

  /// Timeout for sending.
  Duration sendTimeout_;

  /// Timeout for receiving.
  Duration recvTimeout_;
}
