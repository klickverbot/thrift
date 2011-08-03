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
module thrift.server.transport.ssl;

import std.datetime : Duration;
import std.exception : enforce;
import std.socket : Socket;
import thrift.server.transport.socket;
import thrift.transport.base;
import thrift.transport.socket;
import thrift.transport.ssl;

/**
 * A server transport implementation using SSL-encrypted sockets.
 *
 * See: thrift.transport.ssl.
 */
class TSSLServerSocket : TServerSocket {
  /**
   * Creates a new TSSLServerSocket.
   *
   * Params:
   *   port = The port on which to listen.
   *   sslFactory = The TSSLSocketFactory to use for creating client
   *     sockets. Must be in server-side mode.
   */
  this(ushort port, TSSLSocketFactory sslFactory) {
    super(port);
    setSSLFactory(sslFactory);
  }

  /**
   * Creates a new TSSLServerSocket.
   *
   * Params:
   *   port = The port on which to listen.
   *   sendTimeout = The send timeout to set on the client sockets.
   *   recvTimeout = The receive timeout to set on the client sockets.
   *   sslFactory = The TSSLSocketFactory to use for creating client
   *     sockets. Must be in server-side mode.
   */
  this(ushort port, Duration sendTimeout, Duration recvTimeout,
    TSSLSocketFactory sslFactory)
  {
    super(port, sendTimeout, recvTimeout);
    setSSLFactory(sslFactory);
  }

protected:
  override TSocket createTSocket(Socket socket) {
    return sslFactory_.createSocket(socket);
  }

private:
  void setSSLFactory(TSSLSocketFactory sslFactory) {
    enforce(sslFactory.serverSide, new TTransportException(
      "Need server-side SSL socket factory for TSSLServerSocket"));
    sslFactory_ = sslFactory;
  }

  TSSLSocketFactory sslFactory_;
}
