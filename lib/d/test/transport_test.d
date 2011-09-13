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
 * Exercises various transports, combined with the buffered/framed wrappers.
 *
 * In large parts directly ported from the C++ version.
 */
module transport_test;

version (Posix) {} else {
  static assert(false, "This test is Posix-only, because it depends on signals.");
}

import core.time : Duration;
import core.thread : Thread;
import core.sys.posix.unistd : alarm;
import core.sys.posix.signal;
import core.sys.posix.stdlib;
import std.datetime;
import std.file;
import std.getopt;
import std.random : rndGen, uniform, unpredictableSeed;
import std.socket;
import std.stdio;
import std.string;
import std.typetuple;
import thrift.transport.base;
import thrift.transport.buffered;
import thrift.transport.framed;
import thrift.transport.file;
import thrift.transport.http;
import thrift.transport.memory;
import thrift.transport.socket;
import thrift.transport.zlib;


/*
 * Size generation helpers – used to be able to run the same testing code
 * with both constant and random total/chunk sizes.
 */

interface SizeGenerator {
  size_t nextSize();
  string toString();
}

class ConstantSizeGenerator : SizeGenerator {
  this(size_t value) {
    value_ = value;
  }

  override size_t nextSize() {
    return value_;
  }

  override string toString() const {
    return to!string(value_);
  }

private:
  size_t value_;
}

class RandomSizeGenerator : SizeGenerator {
  this(size_t min, size_t max) {
    min_ = min;
    max_ = max;
  }

  override size_t nextSize() {
    return uniform!"[]"(min_, max_);
  }

  override string toString() const {
    return format("rand(%s, %s)", min_, max_);
  }

  size_t min() const @property {
    return min_;
  }

  size_t max() const @property {
    return max_;
  }

private:
  size_t min_;
  size_t max_;
}


/*
 * Classes to set up coupled transports
 */

/**
 * Helper class to represent a coupled pair of transports.
 *
 * Data written to the output transport can be read from the input transport.
 *
 * This is used as the base class for the various coupled transport
 * implementations. It shouldn't be used directly.
 */
class CoupledTransports(Transport) if (isTTransport!Transport) {
  Transport input;
  Transport output;
}

template isCoupledTransports(T) {
  static if (is(T _ : CoupledTransports!U, U)) {
    enum isCoupledTransports = true;
  } else {
    enum isCoupledTransports = false;
  }
}

/**
 * Helper template class for creating coupled transports that wrap
 * another transport.
 */
class CoupledWrapperTransports(WrapperTransport, InnerCoupledTransports) if (
  isTTransport!WrapperTransport && isCoupledTransports!InnerCoupledTransports
) : CoupledTransports!WrapperTransport {
  this() {
    inner_ = new InnerCoupledTransports();
    if (inner_.input) {
      input = new WrapperTransport(inner_.input);
    }
    if (inner_.output) {
      output = new WrapperTransport(inner_.output);
    }
  }

private:
  InnerCoupledTransports inner_;
}

alias Curry!(CoupledWrapperTransports, TBufferedTransport) CoupledBufferedTransports;
alias Curry!(CoupledWrapperTransports, TFramedTransport) CoupledFramedTransports;
alias Curry!(CoupledWrapperTransports, TZlibTransport) CoupledZlibTransports;

/**
 * Coupled TMemoryBuffers.
 */
class CoupledMemoryBuffers : CoupledTransports!TMemoryBuffer {
  this() {
    buf = new TMemoryBuffer;
    input = buf;
    output = buf;
  }

  TMemoryBuffer buf;
}

/**
 * Coupled TSockets.
 */
class CoupledSocketTransports : CoupledTransports!TSocket {
  this() {
    auto sockets = socketPair();
    input = new TSocket(sockets[0]);
    output = new TSocket(sockets[1]);
  }
}

/**
 * Coupled TFileTransports
 */
class CoupledFileTransports : CoupledTransports!TTransport {
  this() {
    fileName_ = to!string(mktemp(cast(char*)toStringz(tmpDir ~
      "/thrift.transport_test.XXXXXX")));

      writefln("Using temp file: %s", fileName_);

    auto writer = new TFileWriterTransport(fileName_);
    writer.open();
    output = writer;

    // Wait until the file has been created.
    writer.flush();

    auto reader = new TFileReaderTransport(fileName_);
    reader.open();
    reader.readTimeout(dur!"seconds"(-1));
    input = reader;
  }

  ~this() {
    // FIXME: Can't remove file, because can't allocate in destructor…
    // remove(fileName_);
  }

  static string tmpDir;

private:
  string fileName_;
}


/*
 * Alarm handling code for use in tests that check the transport blocking
 * semantics.
 *
 * If the timeout expires, we will write additional data to the transport in
 * the SIGALRM handler to unblock it.
 */

struct TriggerInfo {
  this(Duration timeout, TTransport transport, size_t writeLength) {
    this.timeout = timeout;
    this.transport = transport;
    this.writeLength = writeLength;
  }

  Duration timeout;
  TTransport transport;
  size_t writeLength;
  TriggerInfo* next;
}

TriggerInfo* g_triggerInfo;
uint g_numTriggersFired;

extern(C) void alarmHandler(int) {
  // The alarm timed out, which almost certainly means we're stuck
  // on a transport that is incorrectly blocked.
  ++g_numTriggersFired;

  if (g_triggerInfo is null) {
    stderr,writeln("Timeout expired, but trigger stack is empty!");
    exit(1);
  }

  // Pop off the first TriggerInfo.
  // If there is another one, schedule an alarm for it.
  auto info = g_triggerInfo;
  g_triggerInfo = info.next;
  setAlarm();

  // Write some data to the transport to hopefully unblock it.
  auto buf = new ubyte[info.writeLength];
  buf[] = 'b';
  info.transport.write(buf);
  info.transport.flush();
}

void setAlarm() {
  if (g_triggerInfo is null) {
    // clear any alarm
    alarm(0);
    return;
  }

  sigaction_t action;
  action.sa_handler = &alarmHandler;
  action.sa_flags = SA_RESETHAND;
  sigemptyset(&action.sa_mask);
  sigaction(SIGALRM, &action, null);

  alarm(to!uint(g_triggerInfo.timeout.total!"seconds"));
}

/**
 * Add a trigger to be scheduled "seconds" seconds after the
 * last currently scheduled trigger.
 *
 * (Note that this is not "seconds" from now.  That might be more logical, but
 * would require slightly more complicated sorting, rather than just appending
 * to the end.)
 */
void addTrigger(Duration timeout, TTransport transport, size_t writeLen) {
  auto info = new TriggerInfo(timeout, transport, writeLen);

  if (g_triggerInfo is null) {
    // This is the first trigger.
    // Set triggerInfo, and schedule the alarm
    g_triggerInfo = info;
    setAlarm();
  } else {
    // Add this trigger to the end of the list
    auto prev = g_triggerInfo;
    while (prev.next) {
      prev = prev.next;
    }
    prev.next = info;
  }
}

void clearTriggers() {
  alarm(0);
  g_triggerInfo = null;
  g_numTriggersFired = 0;
}

void setTrigger(Duration timeout, TTransport transport, size_t writeLen) {
  clearTriggers();
  addTrigger(timeout, transport, writeLen);
}


/*
 * Test functions
 */

/**
 * Test interleaved write and read calls.
 *
 * Generates a buffer totalSize bytes long, then writes it to the transport,
 * and verifies the written data can be read back correctly.
 *
 * Mode of operation:
 * - call wChunkGenerator to figure out how large of a chunk to write
 *   - call wSizeGenerator to get the size for individual write() calls,
 *     and do this repeatedly until the entire chunk is written.
 * - call rChunkGenerator to figure out how large of a chunk to read
 *   - call rSizeGenerator to get the size for individual read() calls,
 *     and do this repeatedly until the entire chunk is read.
 * - repeat until the full buffer is written and read back,
 *   then compare the data read back against the original buffer
 *
 *
 * - If any of the size generators return 0, this means to use the maximum
 *   possible size.
 *
 * - If maxOutstanding is non-zero, write chunk sizes will be chosen such that
 *   there are never more than maxOutstanding bytes waiting to be read back.
 */
void testReadWrite(CoupledTransports)(
  size_t totalSize,
  SizeGenerator wSizeGenerator,
  SizeGenerator rSizeGenerator,
  SizeGenerator wChunkGenerator,
  SizeGenerator rChunkGenerator,
  size_t maxOutstanding
) if (
  isCoupledTransports!CoupledTransports
) {
  auto transports = new CoupledTransports;
  assert(transports.input);
  assert(transports.output);

  auto wbuf = new ubyte[totalSize];
  auto rbuf = new ubyte[totalSize];

  // Store some data in wbuf.
  foreach (i, ref b; wbuf) {
    b = i & 0xff;
  }

  size_t totalWritten;
  size_t totalRead;
  while (totalRead < totalSize) {
    // Determine how large a chunk of data to write.
    auto wChunkSize = wChunkGenerator.nextSize();
    if (wChunkSize == 0 || wChunkSize > totalSize - totalWritten) {
      wChunkSize = totalSize - totalWritten;
    }

    // Make sure (totalWritten - totalRead) + wChunkSize is less than
    // maxOutstanding.
    if (maxOutstanding > 0 &&
        wChunkSize > maxOutstanding - (totalWritten - totalRead)) {
      wChunkSize = maxOutstanding - (totalWritten - totalRead);
    }

    // Write the chunk.
    size_t chunkWritten = 0;
    while (chunkWritten < wChunkSize) {
      auto writeSize = wSizeGenerator.nextSize();
      if (writeSize == 0 || writeSize > wChunkSize - chunkWritten) {
        writeSize = wChunkSize - chunkWritten;
      }

      transports.output.write(wbuf[totalWritten .. totalWritten + writeSize]);
      chunkWritten += writeSize;
      totalWritten += writeSize;
    }

    // Flush the data, so it will be available in the read transport
    // Don't flush if wChunkSize is 0. (This should only happen if
    // totalWritten == totalSize already, and we're only reading now.)
    if (wChunkSize > 0) {
      transports.output.flush();
    }

    // Determine how large a chunk of data to read back.
    auto rChunkSize = rChunkGenerator.nextSize();
    if (rChunkSize == 0 || rChunkSize > totalWritten - totalRead) {
      rChunkSize = totalWritten - totalRead;
    }

    // Read the chunk.
    size_t chunkRead;
    while (chunkRead < rChunkSize) {
      auto readSize = rSizeGenerator.nextSize();
      if (readSize == 0 || readSize > rChunkSize - chunkRead) {
        readSize = rChunkSize - chunkRead;
      }

      size_t bytesRead;
      try {
        bytesRead = transports.input.read(
          rbuf[totalRead .. totalRead + readSize]);
      } catch (TTransportException e) {
        throw new Exception(format(`read(pos = %s, size = %s) threw ` ~
          `exception "%s"; written so far: %s/%s bytes`, totalRead, readSize,
          e.msg, totalWritten, totalSize));
      }

      enforce(bytesRead > 0, format(`read(pos = %s, size = %s) returned %s; ` ~
        `written so far: %s/%s bytes`, totalRead, readSize, bytesRead,
        totalWritten, totalSize));

      chunkRead += bytesRead;
      totalRead += bytesRead;
    }
  }

  // make sure the data read back is identical to the data written
  if (rbuf != wbuf) {
    stderr.writefln("%s vs. %s", wbuf[$ - 4 .. $], rbuf[$ - 4 .. $]);
    stderr.writefln("rbuf: %s vs. wbuf: %s", rbuf.length, wbuf.length);
  }
  enforce(rbuf == wbuf);
}

void testReadPartAvailable(CoupledTransports)() if (
  isCoupledTransports!CoupledTransports
) {
  auto transports = new CoupledTransports;
  assert(transports.input);
  assert(transports.output);

  ubyte[10] writeBuf = 'a';
  ubyte[10] readBuf;

  // Attemping to read 10 bytes when only 9 are available should return 9
  // immediately.
  transports.output.write(writeBuf[0 .. 9]);
  transports.output.flush();

  setTrigger(dur!"seconds"(3), transports.output, 1);

  auto bytesRead = transports.input.read(readBuf);
  enforce(g_numTriggersFired == 0);
  enforce(bytesRead == 9);

  clearTriggers();
}

void testReadPartialMidframe(CoupledTransports)() if (
  isCoupledTransports!CoupledTransports
) {
  auto transports = new CoupledTransports;
  assert(transports.input);
  assert(transports.output);

  ubyte[13] writeBuf = 'a';
  ubyte[14] readBuf;

  // Attempt to read 10 bytes, when only 9 are available, but after we have
  // already read part of the data that is available.  This exercises a
  // different code path for several of the transports.
  //
  // For transports that add their own framing (e.g., TFramedTransport and
  // TFileTransport), the two flush calls break up the data in to a 10 byte
  // frame and a 3 byte frame.  The first read then puts us partway through the
  // first frame, and then we attempt to read past the end of that frame, and
  // through the next frame, too.
  //
  // For buffered transports that perform read-ahead (e.g.,
  // TBufferedTransport), the read-ahead will most likely see all 13 bytes
  // written on the first read.  The next read will then attempt to read past
  // the end of the read-ahead buffer.
  //
  // Flush 10 bytes, then 3 bytes.  This creates 2 separate frames for
  // transports that track framing internally.
  transports.output.write(writeBuf[0 .. 10]);
  transports.output.flush();
  transports.output.write(writeBuf[10 .. 13]);
  transports.output.flush();

  // Now read 4 bytes, so that we are partway through the written data.
  auto bytesRead = transports.input.read(readBuf[0 .. 4]);
  enforce(bytesRead == 4);

  // Now attempt to read 10 bytes.  Only 9 more are available.
  //
  // We should be able to get all 9 bytes, but it might take multiple read
  // calls, since it is valid for read() to return fewer bytes than requested.
  // (Most transports do immediately return 9 bytes, but the framing transports
  // tend to only return to the end of the current frame, which is 6 bytes in
  // this case.)
  size_t totalRead = 0;
  while (totalRead < 9) {
    setTrigger(dur!"seconds"(3), transports.output, 1);
    bytesRead = transports.input.read(readBuf[4 + totalRead .. 14]);
    enforce(g_numTriggersFired == 0);
    enforce(bytesRead > 0);
    totalRead += bytesRead;
    enforce(totalRead <= 9);
  }

  enforce(totalRead == 9);

  clearTriggers();
}

void testBorrowPartAvailable(CoupledTransports)() if (
  isCoupledTransports!CoupledTransports
) {
  auto transports = new CoupledTransports;
  assert(transports.input);
  assert(transports.output);

  ubyte[9] writeBuf = 'a';
  ubyte[10] readBuf;

  // Attemping to borrow 10 bytes when only 9 are available should return NULL
  // immediately.
  transports.output.write(writeBuf);
  transports.output.flush();

  setTrigger(dur!"seconds"(3), transports.output, 1);

  auto borrowLen = readBuf.length;
  auto borrowedBuf = transports.input.borrow(readBuf.ptr, borrowLen);
  enforce(g_numTriggersFired == 0);
  enforce(borrowedBuf is null);

  clearTriggers();
}

void testReadNoneAvailable(CoupledTransports)() if (
  isCoupledTransports!CoupledTransports
) {
  auto transports = new CoupledTransports;
  assert(transports.input);
  assert(transports.output);

  // Attempting to read when no data is available should either block until
  // some data is available, or fail immediately.  (e.g., TSocket blocks,
  // TMemoryBuffer just fails.)
  //
  // If the transport blocks, it should succeed once some data is available,
  // even if less than the amount requested becomes available.
  ubyte[10] readBuf;

  setTrigger(dur!"seconds"(1), transports.output, 2);
  addTrigger(dur!"seconds"(1), transports.output, 8);

  auto bytesRead = transports.input.read(readBuf);
  if (bytesRead == 0) {
    enforce(g_numTriggersFired == 0);
  } else {
    enforce(g_numTriggersFired == 1);
    enforce(bytesRead == 2);
  }

  clearTriggers();
}

void testBorrowNoneAvailable(CoupledTransports)() if (
  isCoupledTransports!CoupledTransports
) {
  auto transports = new CoupledTransports;
  assert(transports.input);
  assert(transports.output);

  ubyte[16] writeBuf = 'a';

  // Attempting to borrow when no data is available should fail immediately
  setTrigger(dur!"seconds"(1), transports.output, 10);

  auto borrowLen = 10;
  auto borrowedBuf = transports.input.borrow(null, borrowLen);
  enforce(borrowedBuf is null);
  enforce(g_numTriggersFired == 0);

  clearTriggers();
}


void doRwTest(CoupledTransports)(
  size_t totalSize,
  SizeGenerator wSizeGen,
  SizeGenerator rSizeGen,
  SizeGenerator wChunkSizeGen = new ConstantSizeGenerator(0),
  SizeGenerator rChunkSizeGen = new ConstantSizeGenerator(0),
  size_t maxOutstanding = 0
) if (
  isCoupledTransports!CoupledTransports
) {
  totalSize = cast(size_t)(totalSize * g_sizeMultiplier);

  scope(failure) {
    writefln("Test failed for %s: testReadWrite(%s, %s, %s, %s, %s, %s)",
      CoupledTransports.stringof, totalSize, wSizeGen, rSizeGen,
      wChunkSizeGen, rChunkSizeGen, maxOutstanding);
  }

  testReadWrite!CoupledTransports(totalSize, wSizeGen, rSizeGen,
    wChunkSizeGen, rChunkSizeGen, maxOutstanding);
}

void doBlockingTest(CoupledTransports)() if (
  isCoupledTransports!CoupledTransports
) {
  void writeFailure(string name) {
    writefln("Test failed for %s: %s()", CoupledTransports.stringof, name);
  }

  {
    scope(failure) writeFailure("testReadPartAvailable");
    testReadPartAvailable!CoupledTransports();
  }

  {
    scope(failure) writeFailure("testReadPartialMidframe");
    testReadPartialMidframe!CoupledTransports();
  }

  {
    scope(failure) writeFailure("testReadNoneAvaliable");
    testReadNoneAvailable!CoupledTransports();
  }

  {
    scope(failure) writeFailure("testBorrowPartAvailable");
    testBorrowPartAvailable!CoupledTransports();
  }

  {
    scope(failure) writeFailure("testBorrowNoneAvailable");
    testBorrowNoneAvailable!CoupledTransports();
  }
}

SizeGenerator getGenerator(T)(T t) {
  static if (is(T : SizeGenerator)) {
    return t;
  } else {
    return new ConstantSizeGenerator(t);
  }
}

template WrappedTransports(T) if (isCoupledTransports!T) {
  alias TypeTuple!(
    T,
    CoupledBufferedTransports!T,
    CoupledFramedTransports!T,
    CoupledZlibTransports!T
  ) WrappedTransports;
}

void testRw(C, R, S)(
  size_t totalSize,
  R wSize,
  S rSize
) if (
  isCoupledTransports!C && is(typeof(getGenerator(wSize))) &&
  is(typeof(getGenerator(rSize)))
) {
  testRw!C(totalSize, wSize, rSize, 0, 0, 0);
}

void testRw(C, R, S, T, U)(
  size_t totalSize,
  R wSize,
  S rSize,
  T wChunkSize,
  U rChunkSize,
  size_t maxOutstanding = 0
) if (
  isCoupledTransports!C && is(typeof(getGenerator(wSize))) &&
  is(typeof(getGenerator(rSize))) && is(typeof(getGenerator(wChunkSize))) &&
  is(typeof(getGenerator(rChunkSize)))
) {
  foreach (T; WrappedTransports!C) {
    doRwTest!T(
      totalSize,
      getGenerator(wSize),
      getGenerator(rSize),
      getGenerator(wChunkSize),
      getGenerator(rChunkSize),
      maxOutstanding
    );
  }
}

void testBlocking(C)() if (isCoupledTransports!C) {
  foreach (T; WrappedTransports!C) {
    doBlockingTest!T();
  }
}

// A quick hack, for the sake of brevity…
float g_sizeMultiplier = 1;

void main(string[] args) {
  int seed = unpredictableSeed;
  string tmpDir = "/tmp";

  getopt(args, "seed", &seed, "size-multiplier", &g_sizeMultiplier,
    "tmp-dir", &tmpDir);
  enforce(g_sizeMultiplier >= 0, "Size multiplier must not be negative.");

  writefln("Using seed: %s", seed);
  rndGen().seed(seed);
  CoupledFileTransports.tmpDir = tmpDir;

  auto rand4k = new RandomSizeGenerator(1, 4096);

  /*
   * We do the basically the same set of tests for each transport type,
   * although we tweak the parameters in some places.
   */

  // TMemoryBuffer tests
  testRw!CoupledMemoryBuffers(1024 * 1024, 0, 0);
  testRw!CoupledMemoryBuffers(1024 * 256, rand4k, rand4k);
  testRw!CoupledMemoryBuffers(1024 * 256, 167, 163);
  testRw!CoupledMemoryBuffers(1024 * 16, 1, 1);

  testRw!CoupledMemoryBuffers(1024 * 256, 0, 0, rand4k, rand4k);
  testRw!CoupledMemoryBuffers(1024 * 256, rand4k, rand4k, rand4k, rand4k);
  testRw!CoupledMemoryBuffers(1024 * 256, 167, 163, rand4k, rand4k);
  testRw!CoupledMemoryBuffers(1024 * 16, 1, 1, rand4k, rand4k);

  testBlocking!CoupledMemoryBuffers();

  // TSocket tests
  enum socketMaxOutstanding = 4096;
  testRw!CoupledSocketTransports(1024 * 1024, 0, 0,
          0, 0, socketMaxOutstanding);
  testRw!CoupledSocketTransports(1024 * 256, rand4k, rand4k,
          0, 0, socketMaxOutstanding);
  testRw!CoupledSocketTransports(1024 * 256, 167, 163,
          0, 0, socketMaxOutstanding);
  // Doh.  Apparently writing to a socket has some additional overhead for
  // each send() call.  If we have more than ~400 outstanding 1-byte write
  // requests, additional send() calls start blocking.
  testRw!CoupledSocketTransports(1024 * 16, 1, 1,
          0, 0, 400);
  testRw!CoupledSocketTransports(1024 * 256, 0, 0,
          rand4k, rand4k, socketMaxOutstanding);
  testRw!CoupledSocketTransports(1024 * 256, rand4k, rand4k,
          rand4k, rand4k, socketMaxOutstanding);
  testRw!CoupledSocketTransports(1024 * 256, 167, 163,
          rand4k, rand4k, socketMaxOutstanding);
  testRw!CoupledSocketTransports(1024 * 16, 1, 1,
          rand4k, rand4k, 400);

  testBlocking!CoupledSocketTransports();

  // File transport tests.

  // Cannot write more than the frame size at once.
  enum maxWriteAtOnce = 1024 * 1024 * 16 - 4;

  testRw!CoupledFileTransports(1024 * 1024, maxWriteAtOnce, 0);
  testRw!CoupledFileTransports(1024 * 256, rand4k, rand4k);
  testRw!CoupledFileTransports(1024 * 256, 167, 163);
  testRw!CoupledFileTransports(1024 * 16, 1, 1);

  testRw!CoupledFileTransports(1024 * 256, 0, 0, rand4k, rand4k);
  testRw!CoupledFileTransports(1024 * 256, rand4k, rand4k, rand4k, rand4k);
  testRw!CoupledFileTransports(1024 * 256, 167, 163, rand4k, rand4k);
  testRw!CoupledFileTransports(1024 * 16, 1, 1, rand4k, rand4k);

  testBlocking!CoupledFileTransports();
}

private {
  /*
   * Curries a given template by tying the first n arguments to particular
   * types (where n is the number of types passed to Curry).
   *
   * Example:
   * ---
   * struct Foo(T, U, V) {}
   * alias Curry!(Foo, A, B) CurriedFoo;
   * assert(is(CurriedFoo!(C) == Foo!(A, B, C)));
   * ---
   */
  template Curry(alias Target, T...) {
      template Curry(U...) {
          alias Target!(T, U) Curry;
      }
  }
  unittest {
      struct Test(T, U, V) {}
      alias Curry!(Test, Foo, Bar) CurriedTest;
      static assert(is(CurriedTest!Baz == Test!(Foo, Bar, Baz)));
  }
}
