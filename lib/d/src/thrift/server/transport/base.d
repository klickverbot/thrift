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
module thrift.server.transport.base;

import thrift.transport.base;

/**
 * An interface enabling servers to listen for incoming client connections and
 * to create TTransports for them.
 */
abstract class TServerTransport {
  /**
   * Starts listening for server connections.
   *
   * As usual, does not block.
   */
  abstract void listen();

  /**
   * Accepts a client connection and returns an opened TTransport for it,
   * never returning null.
   *
   * Like your average accept() call, blocks until a client connection is
   * available.
   *
   * Throws: TTransportException if accepting failed.
   */
  final TTransport accept() {
    auto transport = acceptImpl();
    if (transport is null) {
      throw new TTransportException("accept() may not return null.");
    }
    return transport;
  }

  /**
   * Closes the server transport, causing it to stop listening.
   */
  abstract void close();

  /**
   * Optional method implementation. This signals to the server transport
   * that it should break out of any accept() or listen() that it is currently
   * blocked on. This method, if implemented, MUST be thread safe, as it may
   * be called from a different thread context than the other TServerTransport
   * methods.
   */
  void interrupt() {}

protected:
  abstract TTransport acceptImpl();
}
