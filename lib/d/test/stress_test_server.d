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
module stress_test_server;

import std.getopt;
import std.stdio;
import thrift.codegen;
import thrift.hashset;
import thrift.protocol.binary;
import thrift.server.base;
import thrift.server.simple;
import thrift.server.threaded;
import thrift.server.transport.socket;
import thrift.transport.buffered;
import thrift.transport.socket;

import thrift.test.stress.Service;

class ServiceHandler : public Service {
  void echoVoid() { return; }
  byte echoByte(byte arg) { return arg; }
  int echoI32(int arg) { return arg; }
  long echoI64(long arg) { return arg; }
  byte[] echoList(byte[] arg) { return arg; }
  HashSet!byte echoSet(HashSet!byte arg) { return arg; }
  byte[byte] echoMap(byte[byte] arg) { return arg; }

  string echoString(string arg) {
    if (arg != "hello") {
      stderr.writefln(`Wrong string received: %s instead of "hello"`, arg);
      throw new Exception("Wrong string received.");
    }
    return arg;
  }
}

enum ServerType {
  simple,
  threaded
}

void main(string[] args) {
  ushort port = 9091;
  auto serverType = ServerType.threaded;

  getopt(args, "port", &port, "server-type", &serverType);

  auto processor = new TServiceProcessor!Service(new ServiceHandler());
  auto serverSocket = new TServerSocket(port);
  auto transportFactory = new TBufferedTransportFactory;
  auto protocolFactory = new TBinaryProtocolFactory!TBufferedTransport;

  TServer server;
  final switch (serverType) {
    case ServerType.simple:
      server = new TSimpleServer(processor, serverSocket,
        transportFactory, protocolFactory);
      break;
    case ServerType.threaded:
      server = new TThreadedServer(processor, serverSocket,
        transportFactory, protocolFactory);
      break;
  }

  writefln("Starting %s server on port %s...", serverType, port);
  server.serve();
  writeln("done.");
}
