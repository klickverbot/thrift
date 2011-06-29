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
module thrift.server.base;

import std.variant : Variant;
import thrift.protocol.base;
import thrift.protocol.binary;
import thrift.protocol.processor;
import thrift.server.transport.base;
import thrift.transport.base;

/**
 * Base class for all Thrift servers.
 *
 * By setting the eventHandler property to a TServerEventHandler
 * implementation, custom code can be integrated into the processing pipeline,
 * which can be used e.g. for gathering statistics.
 */
class TServer {
  abstract void serve();

  void stop() {}

  TProcessor processor;
  TServerTransport serverTransport;
  TTransportFactory inputTransportFactory;
  TTransportFactory outputTransportFactory;
  TProtocolFactory inputProtocolFactory;
  TProtocolFactory outputProtocolFactory;
  TServerEventHandler eventHandler;

protected:
  this(TProcessor processor) {
    this.processor = processor;
    this.inputTransportFactory = new TTransportFactory();
    this.outputTransportFactory = new TTransportFactory();
    this.inputProtocolFactory = new TBinaryProtocolFactory!();
    this.outputProtocolFactory = new TBinaryProtocolFactory!();
  }

  this(TProcessor processor, TServerTransport serverTransport) {
    this(processor);
    this.serverTransport = serverTransport;
  }

  this(
    TProcessor processor,
    TServerTransport serverTransport,
    TTransportFactory transportFactory,
    TProtocolFactory protocolFactory
  ) {
    this.processor = processor;
    this.serverTransport = serverTransport;
    this.inputTransportFactory = transportFactory;
    this.outputTransportFactory = transportFactory;
    this.inputProtocolFactory = protocolFactory;
    this.outputProtocolFactory = protocolFactory;
  }

  this(
    TProcessor processor,
    TServerTransport serverTransport,
    TTransportFactory inputTransportFactory,
    TTransportFactory outputTransportFactory,
    TProtocolFactory inputProtocolFactory,
    TProtocolFactory outputProtocolFactory
  ) {
    this.processor = processor;
    this.serverTransport = serverTransport;
    this.inputTransportFactory = inputTransportFactory;
    this.outputTransportFactory = outputTransportFactory;
    this.inputProtocolFactory = inputProtocolFactory;
    this.outputProtocolFactory = outputProtocolFactory;
  }
}

/**
 * Handles events from a TServer core.
 */
interface TServerEventHandler {
  /**
   * Called before the server starts accepting connections.
   */
  void preServe();

  /**
   * Called when a new client has connected and processing is about to begin.
   */
  Variant createContext(TProtocol input, TProtocol output);

  /**
   * Called when request handling for a client has been finished – can be used
   * to perform clean up work.
   */
  void deleteContext(Variant serverContext, TProtocol input, TProtocol output);

  /**
   * Called when the processor for a client call is about to be invoked.
   */
  void preProcess(Variant serverContext, TTransport transport);
}
