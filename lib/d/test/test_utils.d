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

/**
 * Various helpers used by more than a single test.
 */
module test_utils;

import std.parallelism : TaskPool, totalCPUs;
import thrift.protocol.base;
import thrift.protocol.processor;
import thrift.server.base;
import thrift.server.simple;
import thrift.server.taskpool;
import thrift.server.threaded;
import thrift.server.transport.base;
import thrift.transport.base;

enum ServerType {
  simple,
  taskpool,
  threaded
}

TServer createServer(ServerType type, size_t taskPoolSize,
  TProcessor processor, TServerTransport serverTransport,
  TTransportFactory transportFactory, TProtocolFactory protocolFactory)
{
  final switch (type) {
    case ServerType.simple:
      return new TSimpleServer(processor, serverTransport,
        transportFactory, protocolFactory);
    case ServerType.taskpool:
      auto tps = new TTaskPoolServer(processor, serverTransport,
        transportFactory, protocolFactory);
      tps.setTaskPool(new TaskPool(taskPoolSize));
      return tps;
    case ServerType.threaded:
      return new TThreadedServer(processor, serverTransport,
        transportFactory, protocolFactory);
  }
}
