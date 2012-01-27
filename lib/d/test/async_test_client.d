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
module async_test_client;

import std.conv : to;
import std.datetime;
import std.exception : collectException, enforce;
import std.getopt;
import std.stdio;
import std.string;
import thrift.async.libevent;
import thrift.async.socket;
import thrift.codegen.async_client;
import thrift.codegen.async_client_pool;
import thrift.protocol.binary;
import thrift.transport.base;
import thrift.transport.buffered;
import async_test_common;

void main(string[] args) {
  string host = "localhost";
  ushort port = 9090;
  uint numIterations = 1;
  bool trace;

  getopt(args,
    "n|iterations", &numIterations,
    "trace", &trace,
    "host", (string _, string value) {
      auto parts = split(value, ":");
      if (parts.length > 1) {
        // IPv6 addresses can contain colons, so take the last part for the
        // port.
        host = join(parts[0 .. $ - 1], ":");
        port = to!ushort(parts[$ - 1]);
      } else {
        host = value;
      }
    }
  );

  scope asyncManager = new TLibeventAsyncManager;
  auto socket = new TAsyncSocket(asyncManager, host, port);
  auto client = new TAsyncClient!AsyncTest(
    socket,
    new TBufferedTransportFactory,
    new TBinaryProtocolFactory!TBufferedTransport
  );

  foreach (i; 0 .. numIterations) {
    socket.open();

    {
      if (trace) write(`Calling echo("foo")... `);
      auto a = client.echo("foo");
      enforce(a == "foo");
      if (trace) writeln(`done.`);
    }

    {
      if (trace) write(`Calling delayedEcho("bar", 100 ms)... `);
      auto a = client.delayedEcho("bar", 100);
      enforce(!a.completion.wait(dur!"msecs"(10)), "wait() succeded early.");
      enforce(!a.completion.wait(dur!"msecs"(10)), "wait() succeded early.");
      enforce(a.completion.wait(dur!"msecs"(100)), "wait() didn't succeed as expected.");
      enforce(a.get() == "bar");
      if (trace) writeln(`done.`);
    }

    {
      if (trace) write(`Calling fail("foo")... `);
      auto a = cast(AsyncTestException)collectException(client.fail("foo").waitGet());
      enforce(a && a.reason == "foo");
      if (trace) writeln(`done.`);
    }

    {
      if (trace) write(`Calling delayedFail("bar", 100 ms)... `);
      auto a = client.delayedFail("bar", 100);
      enforce(!a.completion.wait(dur!"msecs"(10)), "wait() succeded early.");
      enforce(!a.completion.wait(dur!"msecs"(10)), "wait() succeded early.");
      enforce(a.completion.wait(dur!"msecs"(100)), "wait() didn't succeed as expected.");
      auto e = cast(AsyncTestException)collectException(a.get());
      enforce(e && e.reason == "bar");
      if (trace) writeln(`done.`);
    }

    {
      socket.recvTimeout = dur!"msecs"(50);

      if (trace) write(`Calling delayedEcho("socketTimeout", 100 ms)... `);
      auto a = client.delayedEcho("socketTimeout", 100);
      auto e = cast(TTransportException)collectException(a.waitGet());
      enforce(e && e.type == TTransportException.Type.TIMED_OUT);
      if (trace) writeln(`timed out as expected.`);

      socket.recvTimeout = dur!"hnsecs"(0);
    }

    socket.close();
  }

  writeln("All tests done.");
}
