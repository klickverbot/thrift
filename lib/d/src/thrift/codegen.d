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
module thrift.codegen;

import std.algorithm : find, max;
import std.array : empty, front;
import std.conv : to;
import std.exception : enforce;
import thrift.base;
import thrift.protocol.base;

/**
 * Struct field requirement levels.
 */
enum TReq {
  OPTIONAL,
  REQUIRED
}

/**
 * Compile-time metadata for a struct field.
 */
struct TFieldMeta {
  /// The name of the field.
  string name;

  /// The (Thrift) id of the field.
  short id;

  /// Whether the field is requried.
  TReq req;

  /// A code string containing a D expression for the default value, if there
  /// is one.
  string defaultValue;
}

mixin template TStructHelpers(alias fieldMetaData = cast(TFieldMeta[])null) if (
  is(typeof(fieldMetaData) : TFieldMeta[])
) {
  import std.algorithm : canFind;

  alias typeof(this) This;

  static if (is(TIsSetFlags!(This, fieldMetaData))) {
    // If we need to keep isSet flags around, create an instance of the
    // container struct.
    TIsSetFlags!(This, fieldMetaData) isSetFlags;
  }

  void set(string fieldName)(MemberType!(This, fieldName) value) if (
    is(MemberType!(This, fieldName))
  ) {
    __traits(getMember, this, fieldName) = value;
    static if (is(typeof(mixin("this.isSetFlags." ~ fieldName)) : bool)) {
      __traits(getMember, this.isSetFlags, fieldName) = true;
    }
  }

  void unset(string fieldName)() if (is(MemberType!(This, fieldName))) {
    static if (isNullable!(MemberType!(This, fieldName))) {
      __traits(getMember, this, fieldName) = null;
    } else static if (is(typeof(mixin("this.isSetFlags." ~ fieldName)) : bool)) {
      __traits(getMember, this.isSetFlags, fieldName) = false;
    }
  }

  bool isSet(string fieldName)() const if (is(MemberType!(This, fieldName))) {
    static if (isNullable!(MemberType!(This, fieldName))) {
      return __traits(getMember, this, fieldName) !is null;
    } else static if (is(typeof(mixin("this.isSetFlags." ~ fieldName)) : bool)) {
      return __traits(getMember, this.isSetFlags, fieldName);
    } else {
      static assert(false, "Thrift internal error, cannot check if " ~
        fieldName ~ " is set.");
    }
  }

  // TODO: opEquals, …

  static if (canFind!`!a.defaultValue.empty`(fieldMetaData)) {
    // DMD @@BUG@@: Have to use auto here to avoid »no size yet for forward
    // reference« errors.
    static auto opCall() {
      auto result = This.init;

      // Generate code for assigning to result from default value strings.
      mixin({
        string code;
        foreach (field; fieldMetaData) {
          if (field.defaultValue.empty) continue;
          code ~= "result." ~ field.name ~ " = " ~ field.defaultValue ~ ";";
        }
        return code;
      }());

      return result;
    }
  }

  void read(TProtocol proto) {
    readStruct!(This, fieldMetaData)(this, proto);
  }

  void write(TProtocol proto) const {
    writeStruct!(This, fieldMetaData)(this, proto);
  }
}

/**
 * Generates an eponymous struct with flags for the optional non-nullable
 * fields of T, if any, or nothing otherwise,
 */
template TIsSetFlags(T, alias fieldMetaData) {
  mixin({
    string boolDefinitions;
    foreach (name; __traits(derivedMembers, T)) {
      static if (!is(MemberType!(T, name))) {
        // We hit something strange like the ThriftStructAdditions template,
        // just ignore.
      } else {
        // If the field is nullable, we don't need an isSet flag as we can map
        // unset to null.
        static if (isNullable!(MemberType!(T, name))) continue;

        auto meta = find!`a.name == b`(fieldMetaData, name);
        if (!meta.empty && meta.front.req == TReq.REQUIRED) continue;
        boolDefinitions ~= "bool " ~ name ~ ";\n";
      }
    }
    if (!boolDefinitions.empty) {
      return "struct TIsSetFlags {\n" ~ boolDefinitions ~ "}";
    } else {
      return "";
    }
  }());
}

/**
 * Reads a Thrift struct to a target protocol.
 *
 * This is defined outside ThriftStructAdditions to make it possible to read
 * exisiting structs from the wire without altering the types.
 */
void readStruct(T, alias fieldMetaData = cast(TFieldMeta[])null)
  (ref T s, TProtocol p)
{
  mixin({
    string code;

    // Check that all fields for which there is meta info are actually in the
    // passed struct type.
    foreach (field; fieldMetaData) {
      code ~= "static assert(is(MemberType!(T, `" ~ field.name ~ "`)));\n";
    }

    string readFieldCode(F)(string name, short id, TReq req) {
      immutable v = "s." ~ name;

      static if (is(F == bool)) {
        immutable pCall = v ~ " = p.readBool();";
      } else static if (is(F == byte)) {
        immutable pCall = v ~ " = p.readByte();";
      } else static if (is(F == double)) {
        immutable pCall = v ~ " = p.readDouble();";
      } else static if (is(F == short)) {
        immutable pCall = v ~ " = p.readI16();";
      } else static if (is(F == int)) {
        immutable pCall = v ~ " = p.readI32();";
      } else static if (is(F == long)) {
        immutable pCall = v ~ " = p.readI64();";
      } else static if (is(F : string)) {
        immutable pCall = v ~ " = p.readString();";
      } else static if (is(F == enum)) {
        immutable pCall = v ~ " = cast(typeof(" ~ v ~ "))p.readI32();";
      } else static if (is(F _ : U[], U)) {
        immutable pCall = "assert(false); // Not implemented yet.";
      } else static if (is(F _ : V[K], K, V)) {
        immutable pCall = "assert(false); // Not implemented yet.";
      } else /+ TODO: sets+/ static if (is(F == struct)) {
        immutable pCall = "assert(false); // Not implemented yet.";
      } else {
        static assert(false, "Cannot represent type in Thrift: " ~ F.stringof);
      }

      string code = "case " ~ to!string(id) ~ ":\n";
      code ~= "if (f.type == " ~ dToTTypeString!F ~ ") {";
      code ~= pCall ~ "\n";
      if (req == TReq.REQUIRED) {
        code ~= "isSet" ~ name ~ " = true;\n";
      }
      code ~= "} else skip(p, f.type);\n";
      code ~= "break;\n";
      return code;
    }

    string isSetFlagCode = "";
    string isSetCheckCode = "";
    string readMembersCode = "";
    short lastId;

    foreach (name; __traits(derivedMembers, T)) {
      static if (is(MemberType!(T, name))) {
        auto meta = find!`a.name == b`(fieldMetaData, name);
        if (meta.empty) continue;

        if (meta.front.req == TReq.REQUIRED) {
          // For required fields, generate bool flags to keep track whether
          // the field has been encountered.
          immutable n = "isSet" ~ name;
          isSetFlagCode ~= "bool " ~ n ~ ";\n";
          isSetCheckCode ~= "enforce(" ~ n ~ ", new TProtocolException(" ~
            "`TRequired field '" ~ name ~ "' not found in serialized data`, " ~
            "TProtocolException.Type.INVALID_DATA));\n";
        }
        readMembersCode ~= readFieldCode!(MemberType!(T, name))(
          name, meta.front.id, meta.front.req);
        lastId = max(lastId, meta.front.id);
      }
    }

    foreach (name; __traits(derivedMembers, T)) {
      static if (is(MemberType!(T, name))) {
        auto meta = find!`a.name == b`(fieldMetaData, name);
        if (!meta.empty) continue;

        ++lastId;
        version (ThriftVerbose) {
          code ~= "pragma(msg, `Warning: No meta information for field '" ~
            name ~ "' in struct '" ~ T.stringof ~ "'. Assigned id: " ~
            to!string(lastId) ~ ".`);\n";
        }
        readMembersCode ~= readFieldCode!(MemberType!(T, name))(
          name, lastId, TReq.OPTIONAL);
      }
    }

    code ~= isSetFlagCode;
    code ~= "p.readStruct((TField f) {\n";
    if (!readMembersCode.empty) {
      code ~= "switch(f.id) {\n";
      code ~= readMembersCode;
      code ~= "default: skip(p, f.type);\n";
      code ~= "}\n";
    }
    code ~= "});\n";
    code ~= isSetCheckCode;

    return code;
  }());
}

/**
 * Writes a Thrift struct to a target protocol.
 *
 * This is defined outside ThriftStructAdditions to make it possible to write
 * exisiting structs without extending them.
 */
void writeStruct(T, alias fieldMetaData = cast(TFieldMeta[])null)
  (const T s, TProtocol p)
{
  // Check that all fields for which there is meta info are actually in the
  // passed struct type.
  mixin({
    string code = "";
    foreach (field; fieldMetaData) {
      code ~= "static assert(is(MemberType!(T, `" ~ field.name ~ "`)));\n";
    }
    return code;
  }());

  // Check that required nullable members are non-null.
  foreach (name; __traits(derivedMembers, T)) {
    static if (!is(MemberType!(T, name))) {
      // We hit something strange like the ThriftStructAdditions template,
      // just ignore.
    } else {
      // If the field is nullable, we don't need an isSet flag as we can map
      // unset to null.
      static if (isNullable!(MemberType!(T, name))) {
        enum meta = find!`a.name == b`(fieldMetaData, name);
        static if (!meta.empty && meta.front.req == TReq.REQUIRED) {
          enforce(__traits(getMember, s, name) !is null,
            new TException("TRequired field '" ~ name ~ "' null."));
        }
      }
    }
  }

  p.writeStruct(TStruct(T.stringof), {
    mixin({
      string writeFieldCode(F)(string name, short id, TReq req) {
        immutable v = "s." ~ name;
        static if (is(F == bool)) {
          immutable pCall = "p.writeBool(" ~ v ~ ");";
        } else static if (is(F == byte)) {
          immutable pCall = "p.writeByte(" ~ v ~ ");";
        } else static if (is(F == double)) {
          immutable pCall = "p.writeDouble(" ~ v ~ ");";
        } else static if (is(F == short)) {
          immutable pCall = "p.writeI16(" ~ v ~ ");";
        } else static if (is(F == int)) {
          immutable pCall = "p.writeI32(" ~ v ~ ");";
        } else static if (is(F == long)) {
          immutable pCall = "p.writeI64(" ~ v ~ ");";
        } else static if (is(F : string)) {
          immutable pCall = "p.writeString(" ~ v ~ ");";
        } else static if (is(F == enum)) {
          immutable pCall = "p.writeI32(cast(int)" ~ v ~ ");";
        } else static if (is(F _ : U[], U)) {
          immutable pCall = "assert(false); // Not implemented yet.";
        } else static if (is(F _ : V[K], K, V)) {
          immutable pCall = "assert(false); // Not implemented yet.";
        } else /+ TODO: sets+/ static if (is(F == struct)) {
          immutable pCall = "assert(false); // Not implemented yet.";
        } else {
          static assert(false, "Cannot represent type in Thrift: " ~ F.stringof);
        }

        string code;
        if (req == TReq.OPTIONAL) {
          code ~= "if (s.isSet!`" ~ name ~ "`()) {\n";
        }

        code ~= "p.writeField(TField(`" ~ name ~ "`, " ~ dToTTypeString!F ~
          ", " ~ to!string(id) ~ "), { " ~ pCall ~ " });\n";

        if (req == TReq.OPTIONAL) {
          code ~= "}\n";
        }
        return code;
      }

      // Generate code for fields withot meta information later, to be able
      // to infer unoccupied ids.
      string code = "";
      short lastId;
      foreach (name; __traits(derivedMembers, T)) {
        static if (is(MemberType!(T, name))) {
          alias MemberType!(T, name) F;

          auto meta = find!`a.name == b`(fieldMetaData, name);
          if (meta.empty) {
            continue;
          }

          code ~= writeFieldCode!F(name, meta.front.id, meta.front.req);
          lastId = max(lastId, meta.front.id);
        }
      }

      foreach (name; __traits(derivedMembers, T)) {
        static if (is(MemberType!(T, name))) {
          alias MemberType!(T, name) F;

          auto meta = find!`a.name == b`(fieldMetaData, name);
          if (!meta.empty) {
            continue;
          }

          ++lastId;
          version (ThriftVerbose) {
            code ~= "pragma(msg, `Warning: No meta information for field '" ~
              name ~ "' in struct '" ~ T.stringof ~ "'. Assigned id: " ~
              to!string(lastId) ~ ".`);\n";
          }
          code ~= writeFieldCode!F(name, lastId, TReq.OPTIONAL);
        }
      }

      return code;
    }());
  });
}

private {
  /**
   * Returns a D code string containing the matching TType value for a passed
   * D type, e.g. dToTTypeString!byte == "TType.BYTE".
   */
  template dToTTypeString(T) {
    static if (is(T == bool)) {
      enum dToTTypeString = "TType.BOOL";
    } else static if (is(T == byte)) {
      enum dToTTypeString = "TType.BYTE";
    } else static if (is(T == double)) {
      enum dToTTypeString = "TType.DOUBLE";
    } else static if (is(T == short)) {
      enum dToTTypeString = "TType.I16";
    } else static if (is(T == int)) {
      enum dToTTypeString = "TType.I32";
    } else static if (is(T == long)) {
      enum dToTTypeString = "TType.I64";
    } else static if (is(T : string)) {
      enum dToTTypeString = "TType.STRING";
    } else static if (is(T == enum)) {
      enum dToTTypeString = "TType.ENUM";
    } else static if (is(T _ : U[], U)) {
      enum dToTTypeString = "TType.LIST";
    } else static if (is(T _ : V[K], K, V)) {
      enum dToTTypeString = "TType.MAP";
    } else /+ TODO: sets+/ static if (is(T == struct)) {
      enum dToTTypeString = "TType.STRUCT";
    } else {
      static assert(false, "Cannot represent type in Thrift: " ~ T.stringof);
    }
  }

  /*
   * Miscellaneous metaprogramming helpers.
   */
  template isNullable(T) {
    enum isNullable = __traits(compiles, { T t = null; });
  }

  template MemberType(T, string name) {
    alias typeof(__traits(getMember, T.init, name)) MemberType;
  }
}
