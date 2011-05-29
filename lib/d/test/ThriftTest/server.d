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
module server;

import std.algorithm;
import std.getopt;
import std.string;
import std.stdio;
import thrift.base;
import thrift.codegen;
import thrift.hashset;
import thrift.protocol.binary;
import thrift.server.simple;
import thrift.transport.buffered;
import thrift.transport.serversocket;

import common;
import thrift.test.ThriftTest_types;
import thrift.test.ThriftTest;

class TestHandler : ThriftTest {
  override void testVoid() {
    version (Trace) writeln("testVoid()");
  }

  override string testString(string thing) {
    version (Trace) writefln("testString(\"%s\")", thing);
    return thing;
  }

  override byte testByte(byte thing) {
    version (Trace) writefln("testByte(%s)", thing);
    return thing;
  }

  override int testI32(int thing) {
    version (Trace) writefln("testI32(%s)", thing);
    return thing;
  }

  override long testI64(long thing) {
    version (Trace) writefln("testI64(%s)", thing);
    return thing;
  }

  override double testDouble(double thing) {
    version (Trace) writefln("testDouble(%s)", thing);
    return thing;
  }

  override Xtruct testStruct(Xtruct thing) {
    version (Trace) writefln("testStruct({\"%s\", %s, %s, %s})",
      thing.string_thing, thing.byte_thing, thing.i32_thing, thing.i64_thing);
    return thing;
  }

  override Xtruct2 testNest(Xtruct2 nest) {
    auto thing = nest.struct_thing;
    version (Trace) writefln("testNest({%s, {\"%s\", %s, %s, %s}, %s})",
      nest.byte_thing, thing.string_thing, thing.byte_thing, thing.i32_thing,
      thing.i64_thing, nest.i32_thing);
    return nest;
  }

  override int[int] testMap(int[int] thing) {
    version (Trace) writefln("testMap({%s})", thing);
    return thing;
  }

  override HashSet!int testSet(HashSet!int thing) {
    version (Trace) writefln("testSet({%s})",
      join(map!`to!string(a)`(thing[]), ", "));
    return thing;
  }

  override int[] testList(int[] thing) {
    version (Trace) writefln("testList(%s)", thing);
    return thing;
  }

  override Numberz testEnum(Numberz thing) {
    version (Trace) writefln("testEnum(%s)", thing);
    return thing;
  }

  override UserId testTypedef(UserId thing) {
    version (Trace) writefln("testTypedef(%s)", thing);
    return thing;
  }

  override int[int][int] testMapMap(int hello) {
    version (Trace) writefln("testMapMap(%s)", hello);

    int[int] pos;
    int[int] neg;
    for (int i = 1; i < 5; i++) {
      pos[i] = i;
      neg[-i] = -i;
    }

    int[int][int] result;
    result[4] = pos;
    result[-4] = neg;
    return result;
  }

  override Insanity[Numberz][UserId] testInsanity(Insanity argument) {
    version (Trace) writeln("testInsanity()");

    Insanity[Numberz][UserId] insane;

    Xtruct hello;
    hello.string_thing = "Hello2";
    hello.byte_thing = 2;
    hello.i32_thing = 2;
    hello.i64_thing = 2;

    Xtruct goodbye;
    goodbye.string_thing = "Goodbye4";
    goodbye.byte_thing = 4;
    goodbye.i32_thing = 4;
    goodbye.i64_thing = 4;

    Insanity crazy;
    crazy.userMap[Numberz.EIGHT] = 8;
    crazy.xtructs ~= goodbye;

    Insanity looney;
    // The C++ TestServer also assigns these to crazy, but that is probably
    // an oversight.
    looney.userMap[Numberz.FIVE] = 5;
    looney.xtructs ~= hello;

    Insanity[Numberz] first_map;
    first_map[Numberz.TWO] = crazy;
    first_map[Numberz.THREE] = crazy;
    insane[1] = first_map;

    Insanity[Numberz] second_map;
    second_map[Numberz.SIX] = looney;
    insane[2] = second_map;

    version (Trace) {
      write("return = ");
      writeInsanityReturn(insane);
      writeln();
    }

    return insane;
  }

  override Xtruct testMulti(byte arg0, int arg1, long arg2, string[short] arg3,
    Numberz arg4, UserId arg5)
  {
    version (Trace) writeln("testMulti()");
    return Xtruct("Hello2", arg0, arg1, arg2);
  }

  override void testException(string arg) {
    version (Trace) writefln("testException(%s)", arg);
    if (arg == "Xception") {
      auto e = new Xception();
      e.errorCode = 1001;
      e.message = arg;
      throw e;
    } else if (arg == "ApplicationException") {
      throw new TException();
    }
  }

  override Xtruct testMultiException(string arg0, string arg1) {
    version (Trace) writefln("testMultiException(%s, %s)", arg0, arg1);

    if (arg0 == "Xception") {
      auto e = new Xception();
      e.errorCode = 1001;
      e.message = "This is an Xception";
      throw e;
    } else if (arg0 == "Xception2") {
      auto e = new Xception2();
      e.errorCode = 2002;
      e.struct_thing.string_thing = "This is an Xception2";
      throw e;
    } else {
      return Xtruct(arg1);
    }
  }

  override void testOneway(int sleepFor) {
    version (Trace) writefln("testOneway(%s): Sleeping...", sleepFor);
    Thread.sleep(dur!"seconds"(sleepFor));
    version (Trace) writefln("testOneway(%s): done sleeping!", sleepFor);
  }
}

void main(string[] args) {
  ushort port = 9090;
  auto serverType = "simple";

  getopt(args, "port", &port, "server-type", &serverType);

  auto protocolFactory = new TBinaryProtocolFactory();
  auto processor = new TServiceProcessor!ThriftTest(new TestHandler());
  auto transportFactory = new TBufferedTransportFactory();
  auto serverSocket = new TServerSocket(port);

  if (serverType == "simple") {
    auto server = new TSimpleServer(processor, serverSocket,
      transportFactory, protocolFactory);

    writefln("Starting the server on port %s...", port);
    server.serve();
  } else {
    throw new Exception("Unknown server type: " ~ serverType);
  }

  writeln("done.");
}
