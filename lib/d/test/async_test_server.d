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
module thrift_test_server;

import core.thread : dur, Thread;
import std.getopt;
import std.parallelism : TaskPool;
import std.stdio;
import thrift.codegen;
import thrift.protocol.binary;
import thrift.server.simple;
import thrift.server.transport.socket;
import thrift.transport.buffered;
import async_test_common;

class AsyncTestHandler : AsyncTest {
  this(bool trace) {
    trace_ = trace;
  }

  override string echo(string value) {
    if (trace_) writefln(`echo("%s")`, value);
    return value;
  }

  override string delayedEcho(string value, long milliseconds) {
    if (trace_) writef(`delayedEcho("%s", %s ms)... `, value, milliseconds);
    Thread.sleep(dur!"msecs"(milliseconds));
    if (trace_) writeln("returning.");

    return value;
  }

  override void fail(string reason) {
    if (trace_) writefln(`fail("%s")`, reason);
    auto ate = new AsyncTestException;
    ate.reason = reason;
    throw ate;
  }

  override void delayedFail(string reason, long milliseconds) {
    if (trace_) writef(`delayedFail("%s", %s ms)... `, reason, milliseconds);
    Thread.sleep(dur!"msecs"(milliseconds));
    if (trace_) writeln("returning.");

    auto ate = new AsyncTestException;
    ate.reason = reason;
    throw ate;
  }

private:
  bool trace_;
  AsyncTestException ate_;
}

void main(string[] args) {
  ushort port = 9090;
  bool trace;

  getopt(args, "port", &port, "trace", &trace);

  auto serverSocket = new TServerSocket(port);
  auto transportFactory = new TBufferedTransportFactory;
  auto protocolFactory = new TBinaryProtocolFactory!();
  auto processor = new TServiceProcessor!AsyncTest(new AsyncTestHandler(trace));

  auto server = new TSimpleServer(processor, serverSocket, transportFactory,
    protocolFactory); // FIXME: Why didn't TThreadPoolServer work?

  writefln("Starting AsyncTest server on port %s...", port);
  server.serve();
  writeln("done.");
}
