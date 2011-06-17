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
module thrift.protocol.base;

import thrift.base;
import thrift.transport.base;

/**
 * Enumerated definition of the types that the Thrift protocol supports.
 *
 * Take special note of the STOP type which is used specifically to mark
 * the end of a sequence of fields.
 */
enum TType : byte {
  STOP   = 0,
  VOID   = 1,
  BOOL   = 2,
  BYTE   = 3,
  DOUBLE = 4,
  I16    = 6,
  I32    = 8,
  I64    = 10,
  STRING = 11,
  STRUCT = 12,
  MAP    = 13,
  SET    = 14,
  LIST   = 15
}

/**
 * Enumerated definition of the message types that the Thrift protocol
 * supports.
 */
enum TMessageType : byte {
  CALL       = 1,
  REPLY      = 2,
  EXCEPTION  = 3,
  ONEWAY     = 4
}

/**
 * Descriptions of Thrift entities.
 *
 * TODO: Why do we have struct names? Where are they used?
 */
struct TField {
  string name;
  TType type;
  short id;
}

/// ditto
struct TList {
  TType elemType;
  size_t size;
}

/// ditto
struct TMap {
  TType keyType;
  TType valueType;
  size_t size;
}

/// ditto
struct TMessage {
  string name;
  TMessageType type;
  int seqid;
}

/// ditto
struct TSet {
  TType elemType;
  size_t size;
}

/// ditto
struct TStruct {
  string name;
}

/**
 * Abstract class for a Thrift protocol driver. These are all the methods that
 * a protocol must implement. Essentially, there must be some way of reading
 * and writing all the base types, plus a mechanism for writing out structs
 * with indexed fields.
 *
 * TProtocol objects should not be shared across multiple encoding contexts,
 * as they may need to maintain internal state in some protocols (i.e. XML).
 * Note that is is acceptable for the TProtocol module to do its own internal
 * buffered reads/writes to the underlying TTransport where appropriate (i.e.
 * when parsing an input XML stream, reading should be batched rather than
 * looking ahead character by character for a close tag).
 *
 * TODO: What is the proper representation of »binary« in D?
 */
abstract class TProtocol {
  /**
   * Prevent direct instantiation
   */
  @disable TProtocol() {}

  public TTransport getTransport() {
    return trans_;
  }

  /*
   * Writing methods.
   */

  abstract void writeBool(bool b);
  abstract void writeByte(byte b);
  abstract void writeI16(short i16);
  abstract void writeI32(int i32);
  abstract void writeI64(long i64);
  abstract void writeDouble(double dub);
  abstract void writeString(string str);
  abstract void writeBinary(ubyte[] buf);

  abstract void writeMessageBegin(TMessage message);
  abstract void writeMessageEnd();
  abstract void writeStructBegin(TStruct tstruct);
  abstract void writeStructEnd();
  abstract void writeFieldBegin(TField field);
  abstract void writeFieldEnd();
  abstract void writeFieldStop();
  abstract void writeListBegin(TList list);
  abstract void writeListEnd();
  abstract void writeMapBegin(TMap map);
  abstract void writeMapEnd();
  abstract void writeSetBegin(TSet set);
  abstract void writeSetEnd();

  /*
   * Reading methods.
   */

  abstract bool readBool();
  abstract byte readByte();
  abstract short readI16();
  abstract int readI32();
  abstract long readI64();
  abstract double readDouble();
  abstract string readString();
  abstract ubyte[] readBinary();

  abstract TMessage readMessageBegin();
  abstract void readMessageEnd();
  abstract TStruct readStructBegin();
  abstract void readStructEnd();
  abstract TField readFieldBegin();
  abstract void readFieldEnd();
  abstract TList readListBegin();
  abstract void readListEnd();
  abstract TMap readMapBegin();
  abstract void readMapEnd();
  abstract TSet readSetBegin();
  abstract void readSetEnd();

  /**
   * Reset any internal state back to a blank slate. This method only needs to
   * be implemented for stateful protocols.
   */
  void reset() {}

protected:
  this(TTransport trans) {
    trans_ = trans;
  }

  TTransport trans_;
}

interface TProtocolFactory {
  TProtocol getProtocol(TTransport trans);
}

class TProtocolException : TException {
  /**
   * Error codes for the various types of exceptions.
   */
  enum Type {
    UNKNOWN,
    INVALID_DATA,
    NEGATIVE_SIZE,
    SIZE_LIMIT,
    BAD_VERSION,
    NOT_IMPLEMENTED
  }

  this(string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
    this(Type.UNKNOWN, file, line, next);
  }

  this(Type type, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
    string msgForType(Type type) {
      switch (type) {
        case Type.UNKNOWN: return "TProtocolException: Unknown protocol exception";
        case Type.INVALID_DATA: return "TProtocolException: Invalid data";
        case Type.NEGATIVE_SIZE: return "TProtocolException: Negative size";
        case Type.SIZE_LIMIT: return "TProtocolException: Exceeded size limit";
        case Type.BAD_VERSION: return "TProtocolException: Invalid version";
        case Type.NOT_IMPLEMENTED: return "TProtocolException: Not implemented";
        default: return "TProtocolException: (Invalid exception type)";
      }
    }
    this(msgForType(type), type, file, line, next);
  }

  this(string msg, string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    this(msg, Type.UNKNOWN, file, line, next);
  }

  this(string msg, Type type, string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    super(msg, file, line, next);
    type_ = type;
  }

  /**
   * Returns an error code that provides information about the type of error
   * that has occurred.
   *
   * @return Error code
   */
  Type getType() const nothrow {
    return type_;
  }

protected:
  /** Error code */
  Type type_;
}

void skip(TProtocol prot, TType type) {
  switch (type) {
    case TType.BOOL:
      prot.readBool();
      break;

    case TType.BYTE:
      prot.readByte();
      break;

    case TType.I16:
      prot.readI16();
      break;

    case TType.I32:
      prot.readI32();
      break;

    case TType.I64:
      prot.readI64();
      break;

    case TType.DOUBLE:
      prot.readDouble();
      break;

    case TType.STRING:
      prot.readBinary();
      break;

    case TType.STRUCT:
      prot.readStructBegin();
      while (true) {
        auto f = prot.readFieldBegin();
        if (f.type == TType.STOP) break;
        skip(prot, f.type);
        prot.readFieldEnd();
      }
      prot.readStructEnd();
      break;

    case TType.LIST:
      auto l = prot.readListBegin();
      foreach (i; 0 .. l.size) {
        skip(prot, l.elemType);
      }
      prot.readListEnd();
      break;

    case TType.MAP:
      auto m = prot.readMapBegin();
      foreach (i; 0 .. m.size) {
        skip(prot, m.keyType);
        skip(prot, m.valueType);
      }
      prot.readMapEnd();
      break;

    case TType.SET:
      auto s = prot.readSetBegin();
      foreach (i; 0 .. s.size) {
        skip(prot, s.elemType);
      }
      prot.readSetEnd();
      break;

    default:
      break;
  }
}

/**
 * Application level exception.
 *
 * TODO: Replace hand-written read()/write() with thrift.codegen templates.
 */
class TApplicationException : TException {
  enum Type {
    UNKNOWN = 0,
    UNKNOWN_METHOD = 1,
    INVALID_MESSAGE_TYPE = 2,
    WRONG_METHOD_NAME = 3,
    BAD_SEQUENCE_ID = 4,
    MISSING_RESULT = 5,
    INTERNAL_ERROR = 6,
    PROTOCOL_ERROR = 7
  }

  this(Type type, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
    string msgForType(Type type) {
      switch (type) {
        case Type.UNKNOWN: return "TApplicationException: Unknown application exception";
        case Type.UNKNOWN_METHOD: return "TApplicationException: Unknown method";
        case Type.INVALID_MESSAGE_TYPE: return "TApplicationException: Invalid message type";
        case Type.WRONG_METHOD_NAME: return "TApplicationException: Wrong method name";
        case Type.BAD_SEQUENCE_ID: return "TApplicationException: Bad sequence identifier";
        case Type.MISSING_RESULT: return "TApplicationException: Missing result";
        default: return "TApplicationException: (Invalid exception type)";
      };
    }
    this(msgForType(type), type, file, line, next);
  }

  this(string msg, string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    this(msg, Type.UNKNOWN, file, line, next);
  }

  this(string msg, Type type, string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    super(msg, file, line, next);
    type_ = type;
  }

  Type type() @property const {
    return type_;
  }

  void read(TProtocol iprot) {
    iprot.readStructBegin();
    while (true) {
      auto f = iprot.readFieldBegin();
      if (f.type == TType.STOP) break;

      switch (f.id) {
        case 1:
          if (f.type == TType.STRING) {
            msg = iprot.readString();
          } else {
            skip(iprot, f.type);
          }
          break;
        case 2:
          if (f.type == TType.I32) {
            type_ = cast(Type)iprot.readI32();
          } else {
            skip(iprot, f.type);
          }
          break;
        default:
          skip(iprot, f.type);
          break;
      }
    }
    iprot.readStructEnd();
  }

  void write(TProtocol oprot) const {
    oprot.writeStructBegin(TStruct("TApplicationException"));

    if (msg != null) {
      oprot.writeFieldBegin(TField("message", TType.STRING, 1));
      oprot.writeString(msg);
      oprot.writeFieldEnd();
    }

    oprot.writeFieldBegin(TField("type", TType.I32, 2));
    oprot.writeI32(type_);
    oprot.writeFieldEnd();

    oprot.writeFieldStop();
    oprot.writeStructEnd();
  }

private:
  Type type_;
}
