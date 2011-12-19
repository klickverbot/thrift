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
module thrift.internal.test.server;

import core.sync.condition;
import core.sync.mutex;
import core.thread : Thread;
import std.datetime;
import std.exception;
import std.typecons;
import thrift.protocol.binary;
import thrift.protocol.processor;
import thrift.server.base;
import thrift.server.transport.socket;
import thrift.transport.base;
import thrift.util.cancellation;

version(unittest):

void testServeCancel(Server)(void delegate(Server) serverSetup = null) if (
  is(Server : TServer)
) {
  auto proc = new WhiteHole!TProcessor;
  auto tf = new TTransportFactory;
  auto pf = new TBinaryProtocolFactory!();

  // Need a special case for TNonblockingServer which doesn't use
  // TServerTransport.
  static if (__traits(compiles, new Server(proc, 0, tf, pf))) {
    auto server = new Server(proc, 0, tf, pf);
  } else {
    auto server = new Server(proc, new TServerSocket(0), tf, pf);
  }

  if (serverSetup) serverSetup(server);

  auto doneMutex = new Mutex;
  auto doneCondition = new Condition(doneMutex);

  foreach (_; 0 .. 100) {
    auto cancel = new TCancellationOrigin;

    auto serverThread = new Thread({
      server.serve(cancel);
      synchronized (doneMutex) {
        doneCondition.notifyAll();
      }
    });
    serverThread.isDaemon = true;
    serverThread.start();

    Thread.sleep(dur!"msecs"(5));
    synchronized (doneMutex) {
      cancel.trigger();
      enforce(doneCondition.wait(dur!"msecs"(100)));
    }
  }
}
