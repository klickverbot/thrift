module benchmark;

import std.datetime : AutoStart, StopWatch;
import std.math : PI;
import std.stdio;
import thrift.protocol.binary;
import thrift.transport.memory;
import DebugProtoTest_types;

void main() {
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

  auto buf = new TMemoryBuffer();

  enum ITERATIONS = 1_000_000;

  {
    auto sw = StopWatch(AutoStart.yes);
    foreach (i; 0 .. ITERATIONS) {
      buf.reset(120);
      auto prot = new TBinaryProtocol(buf);
      ooe.write(prot);
    }
    sw.stop();

    auto msecs = sw.peek.msecs;
    writefln("Write: %s ms (%s kHz)", msecs, ITERATIONS / msecs);
  }

  auto data = buf.getContents;

  {
    auto sw = StopWatch(AutoStart.yes);
    foreach (i; 0 .. ITERATIONS) {
      auto buf2 = new TMemoryBuffer(data);
      auto prot = new TBinaryProtocol(buf2);
      auto ooe2 = OneOfEach();
      ooe2.read(prot);
    }
    sw.stop();

    auto msecs = sw.peek.msecs;
    writefln(" Read: %s ms (%s kHz)", msecs, ITERATIONS / msecs);
  }
}
