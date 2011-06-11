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
module thrift.server.simple;

// stderr is used for error messages until something more sophisticated is
// implemented.
import std.stdio : stderr, writeln;

import thrift.base;
import thrift.protocol.base;
import thrift.protocol.processor;
import thrift.server.base;
import thrift.server.transport.base;
import thrift.transport.base;

/**
 * This is the most basic simple server. It is single-threaded and runs a
 * continuous loop of accepting a single connection, processing requests on
 * that connection until it closes, and then repeating. It is a good example
 * of how to extend the TServer interface.
 */
class TSimpleServer : TServer {
  this(
    TProcessor processor,
    TServerTransport serverTransport,
    TTransportFactory transportFactory,
    TProtocolFactory protocolFactory
  ) {
    super(processor, serverTransport, transportFactory, protocolFactory);
  }

  this(
    TProcessor processor,
    TServerTransport serverTransport,
    TTransportFactory inputTransportFactory,
    TTransportFactory outputTransportFactory,
    TProtocolFactory inputProtocolFactory,
    TProtocolFactory outputProtocolFactory
  ) {
    super(processor, serverTransport, inputTransportFactory,
      outputTransportFactory, inputProtocolFactory, outputProtocolFactory);
  }

  override void serve() {
    TTransport client;
    TTransport inputTransport;
    TTransport outputTransport;
    TProtocol inputProtocol;
    TProtocol outputProtocol;

    try {
      // Start the server listening
      serverTransport.listen();
    } catch (TTransportException ttx) {
      stderr.writefln("TSimpleServer listen() failed: %s", ttx);
      return;
    }

    // Fetch client from server
    while (!stop_) {
      try {
        client = serverTransport.accept();
        scope(failure) client.close();

        inputTransport = inputTransportFactory.getTransport(client);
        scope(failure) inputTransport.close();

        outputTransport = outputTransportFactory.getTransport(client);
        scope(failure) outputTransport.close();

        inputProtocol = inputProtocolFactory.getProtocol(inputTransport);
        outputProtocol = outputProtocolFactory.getProtocol(outputTransport);
      } catch (TTransportException ttx) {
        stderr.writefln("TServerTransport died on accept: %s", ttx);
        continue;
      } catch (TException tx) {
        stderr.writefln("Some kind of accept exception: %s", tx);
        continue;
      } catch (Exception e) {
        stderr.writefln("Some kind of accept exception: %s", e);
        continue;
      }

      try {
        while(true) {
          if (!processor.process(inputProtocol, outputProtocol) ||
              // Peek ahead, is the remote side closed?
              !inputProtocol.getTransport().peek()) {
            break;
          }
        }
      } catch (TTransportException ttx) {
        stderr.writefln("TSimpleServer client died: $s", ttx);
      } catch (TException tx) {
        stderr.writefln("TSimpleServer exception: $s", tx);
      } catch (Exception e) {
        stderr.writefln("TSimpleServer uncaught exception: %s", e);
      }

      try {
        inputTransport.close();
      } catch (TTransportException ttx) {
        stderr.writefln("TSimpleServer input close failed: %s", ttx);
      }
      try {
        outputTransport.close();
      } catch (TTransportException ttx) {
        stderr.writefln("TSimpleServer output close failed: %s", ttx);
      }
      try {
        client.close();
      } catch (TTransportException ttx) {
        stderr.writefln("TSimpleServer client close failed: %s", ttx);
      }
    }

    if (stop_) {
      try {
        serverTransport.close();
      } catch (TTransportException ttx) {
        stderr.writefln("TServerTransport failed on close: %s", ttx);
      }
      stop_ = false;
    }
  }

  override void stop() {
    stop_ = true;
    serverTransport.interrupt();
  }

protected:
  bool stop_;
}
