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
module client;

import std.conv;
import std.datetime;
import std.exception : enforce;
import std.getopt;
import std.stdio;
import std.string;
import thrift.codegen;
import thrift.hashset;
import thrift.protocol.base;
import thrift.protocol.binary;
import thrift.transport.base;
import thrift.transport.buffered;
import thrift.transport.framed;
import thrift.transport.http;
import thrift.transport.socket;
import thrift.transport.ssl;

import common;
import thrift.test.ThriftTest;
import thrift.test.ThriftTest_types;

enum TransportType {
  buffered,
  framed,
  http
}

void main(string[] args) {
  string host = "localhost";
  ushort port = 9090;
  int numTests = 1;
  bool ssl;
  TransportType transportType;

  getopt(args,
    "numTests|n", &numTests,
    "ssl", &ssl,
    "transport", &transportType,
    "host", (string, string value) {
      auto parts = split(value, ":");
      enforce(parts.length == 1 || parts.length == 2,
        "Host argument must be of form 'host' or 'host:port'.");
      host = parts[0];
      if (parts.length == 2) port = to!ushort(parts[1]);
    }
  );

  TSocket socket;
  TSSLSocketFactory sslFactory;
  if (ssl) {
    sslFactory = new TSSLSocketFactory();
    sslFactory.ciphers = "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH";
    sslFactory.authenticate = true;
    sslFactory.loadTrustedCertificates("./trusted-ca-certificate.pem");
    socket = sslFactory.createSocket(host, port);
  } else {
    socket = new TSocket(host, port);
  }

  TTransport transport;
  final switch (transportType) {
    case TransportType.buffered:
      transport = new TBufferedTransport(socket);
      break;
    case TransportType.framed:
      transport = new TFramedTransport(socket);
      break;
    case TransportType.http:
      transport = new TClientHttpTransport(socket, host, "/service");
      break;
  }

  auto protocol = new TBinaryProtocol(transport);
  auto client = new TClient!ThriftTest(protocol);

  ulong time_min;
  ulong time_max;
  ulong time_tot;

  StopWatch sw;
  foreach(test; 0 .. numTests) {
    sw.start();

    try {
      transport.open();
    } catch (TTransportException ttx) {
      writef("Connect failed: %s", ttx.msg);
      if (ttx.next !is null) {
        writef(". Reason: %s.", ttx.next);
      }
      writeln();
      continue;
    }

    writefln("Test #%s, connect %s:%s", test + 1, host, port);

    try {
      write("testVoid()");
      client.testVoid();
      writeln(" = void");
    } catch (TApplicationException tax) {
      writefln("%s", tax);
    }

    write("testString(\"Test\")");
    string s = client.testString("Test");
    writefln(" = \"%s\"", s);

    write("testByte(1)");
    byte u8 = client.testByte(1);
    writefln(" = %s", u8);

    write("testI32(-1)");
    int i32 = client.testI32(-1);
    writefln(" = %s", i32);

    write("testI64(-34359738368)");
    long i64 = client.testI64(-34359738368L);
    writefln(" = %s", i64);

    write("testDouble(-5.2098523)");
    double dub = client.testDouble(-5.2098523);
    writefln(" = %s", dub);

    write("testStruct({\"Zero\", 1, -3, -5})");
    Xtruct out1;
    out1.string_thing = "Zero";
    out1.byte_thing = 1;
    out1.i32_thing = -3;
    out1.i64_thing = -5;
    auto in1 = client.testStruct(out1);
    writefln(" = {\"%s\", %s, %s, %s}", in1.string_thing, in1.byte_thing,
      in1.i32_thing, in1.i64_thing);

    write("testNest({1, {\"Zero\", 1, -3, -5}), 5}");
    Xtruct2 out2;
    out2.byte_thing = 1;
    out2.struct_thing = out1;
    out2.i32_thing = 5;
    auto in2 = client.testNest(out2);
    in1 = in2.struct_thing;
    writefln(" = {%s, {\"%s\", %s, %s, %s}, %s}", in2.byte_thing,
      in1.string_thing, in1.byte_thing, in1.i32_thing, in1.i64_thing,
      in2.i32_thing);

    int[int] mapout;
    for (int i = 0; i < 5; ++i) {
      mapout[i] = i - 10;
    }
    writef("testMap({%s})", mapout);
    auto mapin = client.testMap(mapout);
    writefln(" = {%s}", mapin);

    auto setout = new HashSet!int;
    for (int i = -2; i < 3; ++i) {
      setout ~= i;
    }
    writef("testSet(%s)", setout);
    auto setin = client.testSet(setout);
    writefln(" = %s", setin);

    int[] listout;
    for (int i = -2; i < 3; ++i) {
      listout ~= i;
    }
    writef("testList(%s)", listout);
    auto listin = client.testList(listout);
    writefln(" = %s", listin);

    {
      write("testEnum(ONE)");
      auto ret = client.testEnum(Numberz.ONE);
      writefln(" = %s", ret);

      write("testEnum(TWO)");
      ret = client.testEnum(Numberz.TWO);
      writefln(" = %s", ret);

      write("testEnum(THREE)");
      ret = client.testEnum(Numberz.THREE);
      writefln(" = %s", ret);

      write("testEnum(FIVE)");
      ret = client.testEnum(Numberz.FIVE);
      writefln(" = %s", ret);

      write("testEnum(EIGHT)");
      ret = client.testEnum(Numberz.EIGHT);
      writefln(" = %s", ret);
    }

    write("testTypedef(309858235082523)");
    UserId uid = client.testTypedef(309858235082523L);
    writefln(" = %s", uid);

    write("testMapMap(1)");
    auto mm = client.testMapMap(1);
    writefln(" = {%s}", mm);

    Insanity insane;
    insane.userMap[Numberz.FIVE] = 5000;
    Xtruct truck;
    truck.string_thing = "Truck";
    truck.byte_thing = 8;
    truck.i32_thing = 8;
    truck.i64_thing = 8;
    insane.xtructs ~= truck;
    write("testInsanity()");
    auto whoa = client.testInsanity(insane);
    write(" = ");
    writeInsanityReturn(whoa);
    writeln();

    {
      try {
        write("client.testException(\"Xception\") =>");
        client.testException("Xception");
        writeln("  void\nFAILURE");
      } catch (Xception e) {
        writefln("  {%s, \"%s\"}", e.errorCode, e.message);
      }

      try {
        write("client.testException(\"success\") =>");
        client.testException("success");
        writeln("  void");
      } catch (Exception e) {
        writeln("  exception\nFAILURE");
      }
    }

    {
      try {
        write("client.testMultiException(\"Xception\", \"test 1\") =>");
        auto result = client.testMultiException("Xception", "test 1");
        writeln("  result\nFAILURE");
      } catch (Xception e) {
        writefln("  {%s, \"%s\"}", e.errorCode, e.message);
      }

      try {
        write("client.testMultiException(\"Xception2\", \"test 2\") =>");
        auto result = client.testMultiException("Xception2", "test 2");
        writeln("  result\nFAILURE");
      } catch (Xception2 e) {
        writefln("  {%s, {\"%s\"}}", e.errorCode, e.struct_thing.string_thing);
      }

      try {
        printf("client.testMultiException(\"success\", \"test 3\") =>");
        auto result = client.testMultiException("success", "test 3");
        writefln("  {{\"%s\"}}", result.string_thing);
      } catch (Exception e) {
        writeln("  exception\nFAILURE");
      }
    }

    // Do not run oneway test when doing multiple iterations, as it blocks the
    // server for three seconds.
    if (numTests == 1) {
      printf("client.testOneway(3) =>");
      auto onewayWatch = StopWatch(AutoStart.yes);
      client.testOneway(3);
      onewayWatch.stop();
      if (onewayWatch.peek().msecs > 200) {
        writefln("  FAILURE - took %s ms", onewayWatch.peek().usecs / 1000.0);
      } else {
        writefln("  success - took %s ms", onewayWatch.peek().usecs / 1000.0);
      }

      // Redo a simple test after the oneway to make sure we aren't "off by
      // one", which would be the case if the server treated oneway methods
      // like normal ones.
      write("re-test testI32(-1)");
      i32 = client.testI32(-1);
      writefln(" = %s", i32);
    }

    // Time metering.
    sw.stop();

    immutable tot = sw.peek().usecs;
    writefln("Total time: %s us", tot);

    time_tot += tot;
    if (time_min == 0 || tot < time_min) {
      time_min = tot;
    }
    if (tot > time_max) {
      time_max = tot;
    }
    transport.close();

    sw.reset();
  }

  writeln("\nAll tests done.");

  if (numTests > 1) {
    auto time_avg = time_tot / numTests;
    writefln("Min time: %s us", time_min);
    writefln("Max time: %s us", time_max);
    writefln("Avg time: %s us", time_avg);
  }
}
