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
module thrift.protocol.binary;

import std.typetuple : allSatisfy, TypeTuple;
import thrift.protocol.base;
import thrift.transport.base;

final class TBinaryProtocol(Transport = TTransport) if (
  isTTransport!Transport
) : TProtocol {
  this(Transport trans, bool strictRead = false, bool strictWrite = true) {
    trans_ = trans;
    strictRead_ = strictRead;
    strictWrite_ = strictWrite;
  }

  Transport getTransport() {
    return trans_;
  }

  void setReadLength(int value) {
    if (value > 0) {
      readLength_ = value;
      checkReadLength_ = true;
    } else {
      checkReadLength_ = false;
    }
  }

  void reset() {}

  /*
   * Writing methods.
   */

  void writeBool(bool b) {
    writeByte(b ? 1 : 0);
  }

  void writeByte(byte b) {
    trans_.write((cast(ubyte*)&b)[0..1]);
  }

  void writeI16(short i16) {
    short net = hostToNet(i16);
    trans_.write((cast(ubyte*)&net)[0..2]);
  }

  void writeI32(int i32) {
    int net = hostToNet(i32);
    trans_.write((cast(ubyte*)&net)[0..4]);
  }

  void writeI64(long i64) {
    long net = hostToNet(i64);
    trans_.write((cast(ubyte*)&net)[0..8]);
  }

  void writeDouble(double dub) {
    ulong bits = hostToNet(*cast(ulong*)(&dub));
    trans_.write((cast(ubyte*)&bits)[0..8]);
  }

  void writeString(string str) {
    auto data = cast(ubyte[])str;
    writeI32(cast(int)data.length);
    trans_.write(data);
  }

  void writeBinary(ubyte[] buf) {
    writeI32(cast(int)buf.length);
    trans_.write(buf);
  }

  void writeMessageBegin(TMessage message) {
    if (strictWrite_) {
      int versn = VERSION_1 | message.type;
      writeI32(versn);
      writeString(message.name);
      writeI32(message.seqid);
    } else {
      writeString(message.name);
      writeByte(message.type);
      writeI32(message.seqid);
    }
  }
  void writeMessageEnd() {}

  void writeStructBegin(TStruct tstruct) {}
  void writeStructEnd() {}

  void writeFieldBegin(TField field) {
    writeByte(field.type);
    writeI16(field.id);
  }
  void writeFieldEnd() {}

  void writeFieldStop() {
    writeByte(TType.STOP);
  }

  void writeListBegin(TList list) {
    assert(list.size <= int.max);
    writeByte(list.elemType);
    writeI32(cast(int)list.size);
  }
  void writeListEnd() {}

  void writeMapBegin(TMap map) {
    assert(map.size <= int.max);
    writeByte(map.keyType);
    writeByte(map.valueType);
    writeI32(cast(int)map.size);
  }
  void writeMapEnd() {}

  void writeSetBegin(TSet set) {
    assert(set.size <= int.max);
    writeByte(set.elemType);
    writeI32(cast(int)set.size);
  }
  void writeSetEnd() {}


  /*
   * Reading methods.
   */

  bool readBool() {
    return readByte() != 0;
  }

  byte readByte() {
    ubyte[1] b;
    read(b);
    return cast(byte)b[0];
  }

  short readI16() {
    IntBuf!short b;
    read(b.bytes);
    return netToHost(b.value);
  }

  int readI32() {
    IntBuf!int b;
    read(b.bytes);
    return netToHost(b.value);
  }

  long readI64() {
    IntBuf!long b;
    read(b.bytes);
    return netToHost(b.value);
  }

  double readDouble() {
    IntBuf!long b;
    read(b.bytes);
    b.value = netToHost(b.value);
    return *cast(double*)(&b.value);
  }

  string readString() {
    return readStringBody(readI32());
  }

  ubyte[] readBinary() {
    int size = readI32();
    checkReadLength(size);

    if (size == 0) {
      return null;
    }

    // TODO: Does borrowing actually buy us anything at all here?
    if (auto borrowBuf = trans_.borrow(null, size)) {
      auto buf = borrowBuf[0..size].dup;
      trans_.consume(size);
      return buf;
    } else {
      auto buf = new ubyte[size];
      trans_.readAll(buf);
      return buf;
    }
  }

  TMessage readMessageBegin() {
    TMessage msg = void;

    int size = readI32();
    if (size < 0) {
      int versn = size & VERSION_MASK;
      if(versn != VERSION_1) throw new TProtocolException(
        TProtocolException.Type.BAD_VERSION, "Bad version in readMessage.");

      msg.type = cast(TMessageType)(size & MESSAGE_TYPE_MASK);
      msg.name = readString();
      msg.seqid = readI32();
    } else {
      if (strictRead_) {
        throw new TProtocolException(TProtocolException.Type.BAD_VERSION,
          "Missing version in readMessage, old client?");
      } else {
        msg.name = readStringBody(size);
        msg.type = cast(TMessageType)(readByte());
        msg.seqid = readI32();
      }
    }

    return msg;
  }
  void readMessageEnd() {}

  TStruct readStructBegin() {
    return TStruct();
  }
  void readStructEnd() {}

  TField readFieldBegin() {
    TField f = void;
    f.type = cast(TType)readByte();
    if (f.type == TType.STOP) return f;
    f.id = readI16();
    return f;
  }
  void readFieldEnd() {}

  TList readListBegin() {
    auto l = TList(cast(TType)readByte(), readI32());
    if (l.size < 0)
      throw new TProtocolException(TProtocolException.Type.NEGATIVE_SIZE);
    return l;
  }
  void readListEnd() {}

  TMap readMapBegin() {
    auto m = TMap(cast(TType)readByte(), cast(TType)readByte(), readI32());
    if (m.size < 0)
      throw new TProtocolException(TProtocolException.Type.NEGATIVE_SIZE);
    return m;
  }
  void readMapEnd() {}

  TSet readSetBegin() {
    auto s = TSet(cast(TType)readByte(), readI32());
    if (s.size < 0)
      throw new TProtocolException(TProtocolException.Type.NEGATIVE_SIZE);
    return s;
  }
  void readSetEnd() {}

private:
  /**
   * Wraps trans_.readAll for length checking.
   */
  void read(ubyte[] buf) {
    assert(buf.length < int.max);
    checkReadLength(cast(int)buf.length);
    trans_.readAll(buf);
  }

  string readStringBody(int size) {
    checkReadLength(size);

    if (size == 0) {
      return null;
    }

    // TODO: Does borrowing actually buy us anything at all here?
    if (auto borrowBuf = trans_.borrow(null, size)) {
      auto str = cast(string)borrowBuf[0..size].idup;
      trans_.consume(size);
      return str;
    } else {
      auto buf = new ubyte[size];
      trans_.readAll(buf);
      return cast(string)buf;
    }
  }

  void checkReadLength(int length) {
    if (length < 0)
      throw new TProtocolException(TProtocolException.Type.NEGATIVE_SIZE);

    if (checkReadLength_) {
      readLength_ -= length;
      if (readLength_ < 0)
        throw new TProtocolException(TProtocolException.Type.SIZE_LIMIT);
    }
  }

  enum MESSAGE_TYPE_MASK = 0x000000ff;
  enum VERSION_MASK = 0xffff0000;
  enum VERSION_1 = 0x80010000;

  bool strictRead_;
  bool strictWrite_;

  int readLength_;
  bool checkReadLength_;

  Transport trans_;
}

/**
 * TBinaryProtocol construction helper to avoid having to explicitly specify
 * the transport type (see D Bugzilla enhancement requet 6082).
 */
TBinaryProtocol!Transport createTBinaryProtocol(Transport)(Transport trans,
  bool strictRead = false, bool strictWrite = true)
{
  return new TBinaryProtocol!Transport(trans, strictRead, strictWrite);
}

unittest {
  import thrift.transport.memory;

  // Check the message header format.
  auto buf = new TMemoryBuffer;
  auto binary = createTBinaryProtocol(buf);
  binary.writeMessageBegin(TMessage("foo", TMessageType.CALL, 0));

  auto header = new ubyte[15];
  buf.readAll(header);
  assert(header == [
    128, 1, 0, 1, // Version 1, TMessageType.CALL
    0, 0, 0, 3, // Method name length
    102, 111, 111, // Method name ("foo")
    0, 0, 0, 0, // Sequence id
  ]);
}

class TBinaryProtocolFactory(Transports...) if (
  allSatisfy!(isTTransport, Transports)
) : TProtocolFactory {
  this (bool strictRead = false, bool strictWrite = true, int readLength = 0) {
    strictRead_ = strictRead;
    strictWrite_ = strictWrite;
    readLength_ = readLength;
  }

  TProtocol getProtocol(TTransport trans) const {
    foreach (Transport; TypeTuple!(Transports, TTransport)) {
      auto concreteTrans = cast(Transport)trans;
      if (concreteTrans) {
        auto p = new TBinaryProtocol!Transport(
          concreteTrans, strictRead_, strictWrite_);
        p.setReadLength(readLength_);
        return p;
      }
    }
    throw new TProtocolException(
      "Passed null transport to TBinaryProtocolFactoy");
  }

protected:
  bool strictRead_;
  bool strictWrite_;
  int readLength_;
}

private {
  union IntBuf(T) {
    ubyte[T.sizeof] bytes;
    T value;
  }

  version (BigEndian) {
    T doNothing(T val) { return val; }
    alias doNothing hostToNet;
    alias doNothing netToHost;
  } else {
    import core.bitop : bswap;
    import std.traits : isIntegral;

    T byteSwap(T)(T t) pure nothrow @trusted if (isIntegral!T) {
      static if (T.sizeof == 2) {
        return cast(T)((t & 0xff) << 8) | cast(T)((t & 0xff00) >> 8);
      } else static if (T.sizeof == 4) {
        return cast(T)bswap(cast(uint)t);
      } else static if (T.sizeof == 8) {
        return cast(T)byteSwap(cast(uint)(t & 0xffffffff)) << 32 |
          cast(T)bswap(cast(uint)(t >> 32));
      } else static assert(false, "Type of size " ~ to!string(T.sizeof) ~ " not supported.");
    }
    alias byteSwap hostToNet;
    alias byteSwap netToHost;
  }

  unittest {
    IntBuf!short s;
    s.bytes = [1, 2];
    s.value = byteSwap(s.value);
    assert(s.bytes == [2, 1]);

    IntBuf!int i;
    i.bytes = [1, 2, 3, 4];
    i.value = byteSwap(i.value);
    assert(i.bytes == [4, 3, 2, 1]);

    IntBuf!long l;
    l.bytes = [1, 2, 3, 4, 5, 6, 7, 8];
    l.value = byteSwap(l.value);
    assert(l.bytes == [8, 7, 6, 5, 4, 3, 2, 1]);
  }
}
