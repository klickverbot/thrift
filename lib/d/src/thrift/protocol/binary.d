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

import std.exception : enforce;
import thrift.protocol.base;
import thrift.transport.base;

// TODO: What about non-Posix systems (Windows)? Inlinabilty?
import core.sys.posix.arpa.inet : htons, ntohs, htonl, ntohl;

class TBinaryProtocol : TProtocol {
  this(TTransport trans, bool strictRead = false, bool strictWrite = true) {
    super(trans);
    strictRead_ = strictRead;
    strictWrite_ = strictWrite;
  }

  /*
   * Writing methods.
   */

  override void writeBool(bool b) {
    writeByte(b ? 1 : 0);
  }

  override void writeByte(byte b) {
    trans_.write((cast(ubyte*)&b)[0..1]);
  }

  override void writeI16(short i16) {
    short net = htons(i16);
    trans_.write((cast(ubyte*)&net)[0..2]);
  }

  override void writeI32(int i32) {
    int net = htonl(i32);
    trans_.write((cast(ubyte*)&net)[0..4]);
  }

  override void writeI64(long i64) {
    long net = htonll(i64);
    trans_.write((cast(ubyte*)&net)[0..8]);
  }

  override void writeDouble(double dub) {
    ulong bits = htonll(*cast(ulong*)(&dub));
    trans_.write((cast(ubyte*)&bits)[0..8]);
  }

  override void writeString(string str) {
    auto data = cast(ubyte[])str;
    writeI32(data.length);
    trans_.write(data);
  }

  override void writeBinary(ubyte[] buf) {
    writeI32(buf.length);
    trans_.write(buf);
  }

  override void writeField(TField field, void delegate() writeContents) {
    writeByte(field.type);
    writeI16(field.id);

    writeContents();
  }

  override void writeList(TList list, void delegate() writeContents) {
    writeByte(list.elemType);
    writeI32(list.size);

    writeContents();
  }

  override void writeMap(TMap map, void delegate() writeContents) {
    writeByte(map.keyType);
    writeByte(map.valueType);
    writeI32(map.size);

    writeContents();
  }

  override void writeMessage(TMessage message, void delegate() writeContents) {
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

    writeContents();
  }

  override void writeSet(TSet set, void delegate() writeContents) {
    writeByte(set.elemType);
    writeI32(set.size);

    writeContents();
  }

  override void writeStruct(TStruct tstruct, void delegate() writeContents) {
    writeContents();
    writeByte(TType.STOP);
  }

  /*
   * Reading methods.
   */

  override bool readBool() {
    return readByte() != 0;
  }

  override byte readByte() {
    ubyte[1] b;
    read(b);
    return cast(byte)b[0];
  }

  override short readI16() {
    IntBuf!short b;
    read(b.bytes);
    return ntohs(b.value);
  }

  override int readI32() {
    IntBuf!int b;
    read(b.bytes);
    return ntohl(b.value);
  }

  override long readI64() {
    IntBuf!long b;
    read(b.bytes);
    return ntohll(b.value);
  }

  override double readDouble() {
    IntBuf!long b;
    read(b.bytes);
    b.value = ntohll(b.value);
    return *cast(double*)(&b.value);
  }

  override string readString() {
    return readStringBody(readI32());
  }

  override ubyte[] readBinary() {
    int size = readI32();
    checkReadLength(size);

    if (size == 0) {
      return null;
    }

    uint got = size;
    // TODO: Does borrowing actually buy us anything at all here?
    if (auto borrowBuf = trans_.borrow(null, got)) {
      auto buf = borrowBuf[0..size].dup;
      trans_.consume(size);
      return buf;
    } else {
      auto buf = new ubyte[size];
      trans_.readAll(buf);
      return buf;
    }
  }

  override TList readList(void delegate(TList) readContents) {
    auto l = TList(cast(TType)readByte(), readI32());
    enforce(l.size >= 0,
      new TProtocolException(TProtocolException.Type.NEGATIVE_SIZE));
    readContents(l);
    return l;
  }

  override TMap readMap(void delegate(TMap) readContents) {
    auto m = TMap(cast(TType)readByte(), cast(TType)readByte(), readI32());
    enforce(m.size >= 0,
      new TProtocolException(TProtocolException.Type.NEGATIVE_SIZE));
    readContents(m);
    return m;
  }

  override TMessage readMessage(void delegate(TMessage) readContents) {
    TMessage msg = void;

    int size = readI32();
    if (size < 0) {
      int versn = size & VERSION_MASK;
      enforce(versn == VERSION_1, new TProtocolException(
        TProtocolException.Type.BAD_VERSION, "Bad version in readMessage."));

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

    readContents(msg);

    return msg;
  }

  override TSet readSet(void delegate(TSet) readContents) {
    auto s = TSet(cast(TType)readByte(), readI32());
    enforce(s.size >= 0,
      new TProtocolException(TProtocolException.Type.NEGATIVE_SIZE));
    readContents(s);
    return s;
  }

  override TStruct readStruct(void delegate(TField) readField) {
    while (true) {
      TField f;
      f.type = cast(TType)readByte();
      if (f.type == TType.STOP) {
        return TStruct();
      }
      f.id = readI16();
      readField(f);
    }
  }

  void setReadLength(int value) {
    readLength_ = value;
    checkReadLength_ = true;
  }

protected:
  /**
   * Wraps trans_.readAll for length checking.
   */
  void read(ubyte[] buf) {
    checkReadLength(buf.length);
    trans_.readAll(buf);
  }

  string readStringBody(int size) {
    checkReadLength(size);

    if (size == 0) {
      return null;
    }

    uint got = size;
    // TODO: Does borrowing actually buy us anything at all here?
    if (auto borrowBuf = trans_.borrow(null, got)) {
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
    enforce(length >= 0,
      new TProtocolException(TProtocolException.Type.NEGATIVE_SIZE));

    if (checkReadLength_) {
      readLength_ -= length;
      enforce(readLength_ >= 0,
        new TProtocolException(TProtocolException.Type.SIZE_LIMIT));
    }
  }

  enum MESSAGE_TYPE_MASK = 0x000000ff;
  enum VERSION_MASK = 0xffff0000;
  enum VERSION_1 = 0x80010000;

  bool strictRead_;
  bool strictWrite_;

  int readLength_;
  bool checkReadLength_;
}

class TBinaryProtocolFactory : TProtocolFactory {
  this (bool strictRead = false, bool strictWrite = true, int readLength = 0) {
    strictRead_ = strictRead;
    strictWrite_ = strictWrite;
    readLength_ = readLength;
  }

  TBinaryProtocol getProtocol(TTransport trans) const {
    auto p = new TBinaryProtocol(trans, strictRead_, strictWrite_);
    if (readLength_ != 0) {
      p.setReadLength(readLength_);
    }
    return p;
  }

protected:
  bool strictRead_;
  bool strictWrite_;
  int readLength_;
}

private {
  ulong htonll(ulong n) {
    version (BigEndian) {
      return n;
    } else {
      ulong r = (cast(ulong) htonl(n & 0xFFFFFFFFLU)) << 32;
      r |= htonl((n & 0xFFFFFFFF00000000LU) >> 32);
      return r;
    }
  }

  ulong ntohll(ulong n) {
    version (BigEndian) {
      return n;
    } else {
      ulong r = (cast(ulong) ntohl(n & 0xFFFFFFFFLU)) << 32;
      r |= ntohl((n & 0xFFFFFFFF00000000LU) >> 32);
      return r;
    }
  }

  union IntBuf(T) {
    ubyte[T.sizeof] bytes;
    T value;
  }
}
