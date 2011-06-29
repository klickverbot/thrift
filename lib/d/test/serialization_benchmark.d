/**
 * An implementation of the mini serialization benchmark also available for
 * C++ and Java.
 *
 * For meaningful results, you might want to make sure that
 * the Thrift library is compiled with release build flags,
 * e.g. by including the source files with the build instead
 * of linking libthriftd:
 *
 * dmd -w -O -release -inline -I../../../../lib/d/src -Igen-d
 * -ofserialization_benchmark $(find ../../src/thrift -name '*.d')
 * gen-d/DebugProtoTest_types.d serialization_benchmark.d
 */
module serialization_benchmark;

import std.datetime : AutoStart, StopWatch;
import std.math : PI;
import std.stdio;
import thrift.protocol.binary;
import thrift.transport.memory;
import DebugProtoTest_types;

void main() {
  auto buf = new TMemoryBuffer;
  enum ITERATIONS = 1_000_000;

  {
    auto ooe = OneOfEach();
    ooe.im_true   = true;
    ooe.im_false  = false;
    ooe.a_bite    = 0x7f;
    ooe.integer16 = 27_000;
    ooe.integer32 = 1 << 24;
    ooe.integer64 = 6_000_000_000;
    ooe.double_precision = PI;
    ooe.some_characters = "JSON THIS! \"\1";
    ooe.zomg_unicode = "\xd7\n\a\t";
    ooe.base64 = "\1\2\3\255";

    auto sw = StopWatch(AutoStart.yes);
    auto prot = createTBinaryProtocol(buf);
    foreach (i; 0 .. ITERATIONS) {
      buf.reset(120);
      ooe.write(prot);
    }
    sw.stop();

    auto msecs = sw.peek.msecs;
    writefln("Write: %s ms (%s kHz)", msecs, ITERATIONS / msecs);
  }

  auto data = buf.getContents();

  {
    auto readBuf = new TMemoryBuffer();
    auto prot = createTBinaryProtocol(readBuf);
    auto ooe = OneOfEach();

    auto sw = StopWatch(AutoStart.yes);
    foreach (i; 0 .. ITERATIONS) {
      // TODO: We copy the data every time here, implement a
      // TMemoryReadingTransport which just operates on a slice of the
      // original data.
      readBuf.write(data);
      ooe.read(prot);
    }
    sw.stop();

    auto msecs = sw.peek.msecs;
    writefln(" Read: %s ms (%s kHz)", msecs, ITERATIONS / msecs);
  }
}
