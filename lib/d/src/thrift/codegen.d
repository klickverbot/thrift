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
import std.traits;
import thrift.base;
import thrift.protocol.base;

/**
 * Struct field requirement levels.
 */
enum TReq {
  OPTIONAL,
  REQUIRED,
  IGNORE /// Ignore the struct field when serializing/deserializing.
}

enum TMethodType {
  REGULAR,
  ONEWAY
}

/**
 * Compile-time metadata for a struct field.
 */
struct TFieldMeta {
  /// The name of the field. Used for matching TFieldMeta with the actual
  /// D struct member.
  string name;

  /// The (Thrift) id of the field.
  short id;

  /// Whether the field is requried.
  TReq req;

  /// A code string containing a D expression for the default value, if there
  /// is one.
  string defaultValue;
}

/**
 * Compile-time metadata for a service method.
 */
struct TMethodMeta {
  string name;
  TParamMeta[] params;
  TExceptionMeta[] exceptions;
  TMethodType type;
}

/**
 * Compile-time metadata for a service method parameter.
 */
struct TParamMeta {
  /// The name of the parameter. Contrary to TFieldMeta, it only serves
  /// decorative purposes here.
  string name;

  short id;

  string defaultValue;
}

/**
 * Compile-time metadata for a service method exception annotation.
 */
struct TExceptionMeta {
  /// The name of the exception »return value«. Contrary to TFieldMeta, it
  /// only serves decorative purposes here.
  string name;

  short id;

  string type;
}

mixin template TStructHelpers(alias fieldMetaData = cast(TFieldMeta[])null) if (
  is(typeof(fieldMetaData) : TFieldMeta[])
) {
  import std.algorithm : canFind;
  import thrift.protocol.base : TProtocol;

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
    // DMD @@BUG@@: Why is false required here?
    writeStruct!(This, fieldMetaData, false)(this, proto);
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
        // We hit something strange like the TStructHelpers template itself,
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
 * This is defined outside TStructHelpers to make it possible to read
 * exisiting structs from the wire without altering the types.
 */
void readStruct(T, alias fieldMetaData = cast(TFieldMeta[])null,
  bool pointerStruct = false)(ref T s, TProtocol p) {
  mixin({
    string code;

    // Check that all fields for which there is meta info are actually in the
    // passed struct type.
    // DMD @@BUG@@: foreach should just be skipped for null arrays, the
    // static if clause should not be necessary.
    static if (fieldMetaData) foreach (field; fieldMetaData) {
      code ~= "static assert(is(MemberType!(T, `" ~ field.name ~ "`)));\n";
    }

    string readFieldCode(FieldType)(string name, short id, TReq req) {
      static if (pointerStruct && isPointer!FieldType) {
        immutable v = "*s." ~ name;
        alias Unqual!(pointerTarget!FieldType) F;
      } else {
        immutable v = "s." ~ name;
        alias Unqual!FieldType F;
      }

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
      } else static if (is(F == struct)) {
        immutable pCall = "assert(false); // Not implemented yet.";
      } else static if (is(F : TException)) {
        immutable pCall = v ~ " = new typeof(" ~ v ~ ")();\n" ~
          v ~ ".read(p);";
      } else {
        static assert(false, "Cannot represent type in Thrift: " ~ F.stringof);
      }

      string code = "case " ~ to!string(id) ~ ":\n";
      code ~= "if (f.type == " ~ dToTTypeString!F ~ ") {";
      code ~= pCall ~ "\n";
      if (req == TReq.REQUIRED) {
        code ~= "isSet" ~ name ~ " = true;\n";
      } else if (!isNullable!F){
        code ~= "s.isSetFlags." ~ name ~ " = true;\n";
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
      static if (is(MemberType!(T, name)) &&
        !isSomeFunction!(MemberType!(T, name)))
      {
        enum meta = find!`a.name == b`(fieldMetaData, name);
        static if (!meta.empty && meta.front.req != TReq.IGNORE) {
          if (meta.front.req == TReq.REQUIRED) {
            // For required fields, generate bool flags to keep track whether
            // the field has been encountered.
            immutable n = "isSet_" ~ name;
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
    }

    foreach (name; __traits(derivedMembers, T)) {
      static if (is(MemberType!(T, name)) &&
        !isSomeFunction!(MemberType!(T, name)))
      {
        enum meta = find!`a.name == b`(fieldMetaData, name);
        static if (meta.empty) {
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
 * This is defined outside TStructHelpers to make it possible to write
 * exisiting structs without extending them.
 */
void writeStruct(T, alias fieldMetaData = cast(TFieldMeta[])null,
  bool pointerStruct = false) (const T s, TProtocol p) {
  // Check that all fields for which there is meta info are actually in the
  // passed struct type.
  mixin({
    string code = "";
    // DMD @@BUG@@: foreach should just be skipped for null arrays, the
    // static if clause should not be necessary.
    static if (fieldMetaData) foreach (field; fieldMetaData) {
      code ~= "static assert(is(MemberType!(T, `" ~ field.name ~ "`)));\n";
    }
    return code;
  }());

  // Check that required nullable members are non-null.
  foreach (name; __traits(derivedMembers, T)) {
    static if (is(MemberType!(T, name)) &&
      !isSomeFunction!(MemberType!(T, name)))
    {
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
      string writeFieldCode(FieldType)(string name, short id, TReq req) {
        static if (pointerStruct && isPointer!FieldType) {
          immutable v = "(*s." ~ name ~ ")";
          alias Unqual!(pointerTarget!FieldType) F;
        } else {
          immutable v = "s." ~ name;
          alias Unqual!FieldType F;
        }

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
          immutable pCall = "assert(false); /+ Not implemented yet.+/";
        } else static if (is(F _ : V[K], K, V)) {
          immutable pCall = "assert(false); /+ Not implemented yet.+/";
        } else static if (is(F == struct)) {
          immutable pCall = v ~ ".write(p);";
        } else {
          static assert(false, "Cannot represent type in Thrift: " ~ F.stringof);
        }

        string code;
        if (!pointerStruct && req == TReq.OPTIONAL) {
          code ~= "if (s.isSet!`" ~ name ~ "`()) {\n";
        }

        code ~= "p.writeField(TField(`" ~ name ~ "`, " ~ dToTTypeString!F ~
          ", " ~ to!string(id) ~ "), { " ~ pCall ~ " });\n";
        // code ~= "writefln(`" ~ name ~ ": %s`, " ~ v ~ ");\n";

        if (!pointerStruct && req == TReq.OPTIONAL) {
          code ~= "}\n";
        }
        return code;
      }

      // Generate code for fields withot meta information later, to be able
      // to infer unoccupied ids.
      string code = "";
      short lastId;
      foreach (name; __traits(derivedMembers, T)) {
        static if (is(MemberType!(T, name)) &&
          !isSomeFunction!(MemberType!(T, name)))
        {
          alias MemberType!(T, name) F;

          auto meta = find!`a.name == b`(fieldMetaData, name);
          if (meta.empty || meta.front.req == TReq.IGNORE) {
            continue;
          }

          code ~= writeFieldCode!F(name, meta.front.id, meta.front.req);
          lastId = max(lastId, meta.front.id);
        }
      }

      foreach (name; __traits(derivedMembers, T)) {
        static if (is(MemberType!(T, name)) &&
          !isSomeFunction!(MemberType!(T, name)))
        {
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

template TPargsStruct(Interface, string methodName) {
  static assert(is(typeof(mixin("Interface." ~ methodName))),
    "Could not find method '" ~ methodName ~ "' in '" ~ Interface.stringof ~ "'.");
  mixin({
    bool methodMetaFound;
    TMethodMeta methodMeta;
    static if (is(typeof(Interface.methodMeta) : TMethodMeta[])) {
      auto meta = find!`a.name == b`(Interface.methodMeta, methodName);
      if (!meta.empty) {
        methodMetaFound = true;
        methodMeta = meta.front;
      }
    }

    string memberCode;
    string[] fieldMetaCodes;
    foreach (i, _; ParameterTypeTuple!(mixin("Interface." ~ methodName))) {
      // If we have no meta information, just use param1, param2, etc. as
      // field names, it shouldn't really matter anyway. 1-based »indexing«
      // is used to match the common scheme in the Thrift world.
      immutable memberName = methodMetaFound ? methodMeta.params[i].name :
        "param" ~ to!string(i + 1);

      // Workaround for DMD @@BUG@@ 6056: make an intermediary alias for the
      // parameter type, and declare the member using const(memberNameType)*.
      memberCode ~= "alias ParameterTypeTuple!(Interface." ~ methodName ~
        ")[" ~ to!string(i) ~ "] " ~ memberName ~ "Type;\n";
      memberCode ~= "const(" ~ memberName ~ "Type)* " ~ memberName ~ ";\n";

      fieldMetaCodes ~= "TFieldMeta(`" ~ memberName ~ "`, " ~
        to!string(methodMetaFound ? methodMeta.params[i].id : (i + 1)) ~ ")";
    }

    string code = "struct TPargsStruct {\n";
    code ~= memberCode;
    version (ThriftVerbose) {
      if (!methodMetaFound) {
        code ~= "pragma(msg, `Warning: No meta information for method '" ~
          methodName ~ "' in service '" ~ Interface.stringof ~ "' found.`);\n";
      }
    }
    code ~= "void write(TProtocol proto) const {\n";
    code ~= "writeStruct!(TPargsStruct, [" ~ ctfeJoin(fieldMetaCodes, ", ") ~
      "], true)(this, proto);\n";
    code ~= "}\n";
    code ~= "}\n";
    return code;
  }());
}

template TPresultStruct(Interface, string methodName) {
  static assert(is(typeof(mixin("Interface." ~ methodName))),
    "Could not find method '" ~ methodName ~ "' in '" ~ Interface.stringof ~ "'.");

  mixin({
    string code = "struct TPresultStruct {\n";

    string[] fieldMetaCodes;

    static if (!is(ReturnType!(mixin("Interface." ~ methodName)) == void)) {
      code ~= q{
        ReturnType!(mixin("Interface." ~ methodName))* success;
        struct IsSetFlags {
          bool success;
        }
        IsSetFlags isSetFlags;
      };
      fieldMetaCodes ~= [
        "TFieldMeta(`success`, 0)",
        "TFieldMeta(`isSetFlags`, 0, TReq.IGNORE)"
      ];
    }

    static if (is(typeof(Interface.methodMeta) : TMethodMeta[])) {
      auto meta = find!`a.name == b`(Interface.methodMeta, methodName);
      if (!meta.empty) {
        // DMD @@BUG@@: The if should not be necessary, but otherwise DMD ICEs
        // on empty exception arrays.
        if (!meta.front.exceptions.empty) foreach (e; meta.front.exceptions) {
          code ~= "Interface." ~ e.type ~ " " ~ e.name ~ ";\n";
          fieldMetaCodes ~= "TFieldMeta(`" ~ e.name ~ "`, " ~ to!string(e.id) ~ ")";
        }
      } else {
        version (ThriftVerbose) {
          code ~= "pragma(msg, `Warning: No meta information for method '" ~
            methodName ~ "' in service '" ~ Interface.stringof ~ "' found.`);\n";
        }
      }
    }

    code ~= q{
      bool isSet(string fieldName)() const if (is(MemberType!(typeof(this), fieldName))) {
        static if (fieldName == "success") {
          return isSetFlags.success;
        } else {
          // We are dealing with an exception member, which, being a nullable
          // type (exceptions are always classes), has no isSet flag.
          return __traits(getMember, this, fieldName) !is null;
        }
      }
    };

    code ~= "void read(TProtocol proto) {\n";
    code ~= "readStruct!(TPresultStruct, [" ~ ctfeJoin(fieldMetaCodes, ", ") ~
      "], true)(this, proto);\n";
    code ~= "}\n";
    code ~= "}\n";
    return code;
  }());
}

template TClient(Interface) if (is(Interface _ == interface)) {
  mixin({
    string code = "class TClient : Interface {";
    static if (is(Interface BaseInterfaces == super) && BaseInterfaces.length > 0) {
      static assert(false, "Service inheritance not implemented yet.");
    } else {
      code ~= q{
        this(TProtocol iprot, TProtocol oprot) {
          iprot_ = iprot;
          oprot_ = oprot;
        }

        this(TProtocol prot) {
          this(prot, prot);
        }

        TProtocol getInputProtocol() {
          return iprot_;
        }

        TProtocol getOutputProtocol() {
          return oprot_;
        }

        protected TProtocol iprot_;
        protected TProtocol oprot_;
        protected int seqid_;
      };
    }

    foreach (methodName; __traits(derivedMembers, Interface)) {
      static if (isSomeFunction!(mixin("Interface." ~ methodName))) {
        bool methodMetaFound;
        TMethodMeta methodMeta;
        static if (is(typeof(Interface.methodMeta) : TMethodMeta[])) {
          enum meta = find!`a.name == b`(Interface.methodMeta, methodName);
          if (!meta.empty) {
            methodMetaFound = true;
            methodMeta = meta.front;
          }
        }

        // Generate the code for sending.
        string[] paramList;
        string paramAssignCode;
        foreach (i, _; ParameterTypeTuple!(mixin("Interface." ~ methodName))) {
          // Just cosmetics in this case.
          immutable paramName = methodMetaFound ? methodMeta.params[i].name :
            "param" ~ to!string(i + 1);

          paramList ~= "ParameterTypeTuple!(Interface." ~ methodName ~ ")[" ~
            to!string(i) ~ "] " ~ paramName;
          paramAssignCode ~= "args." ~ paramName ~ " = &" ~ paramName ~ ";\n";
        }
        code ~= "ReturnType!(Interface." ~ methodName ~ ") " ~ methodName ~
          "(" ~ ctfeJoin(paramList, ", ") ~ ") {\n";

        code ~= "immutable methodName = `" ~ methodName ~ "`;\n";

        immutable paramStructType =
          "TPargsStruct!(Interface, `" ~ methodName ~ "`)";
        code ~= paramStructType ~ " args = " ~ paramStructType ~ "();\n";
        code ~= paramAssignCode;
        code ~= "oprot_.writeMessage(TMessage(`" ~ methodName ~
          "`, TMessageType.CALL, ++seqid_), {\n";
        code ~= "args.write(oprot_);\n";
        code ~= "});\n";
        code ~= "oprot_.getTransport().flush();\n";

        // If this is not a oneway method, generate the recieving code.
        if (!methodMetaFound || methodMeta.type != TMethodType.ONEWAY) {
          code ~= "TPresultStruct!(Interface, `" ~ methodName ~ "`) result;\n";

          if (!is(ReturnType!(mixin("Interface." ~ methodName)) == void)) {
            code ~= "ReturnType!(Interface." ~ methodName ~ ") _return;\n";
            code ~= "result.success = &_return;\n";
          }

          // TODO: The C++ implementation checks for matching name here,
          // should we do as well?
          code ~= q{
            iprot_.readMessage((TMessage msg) {
              if (msg.type == TMessageType.EXCEPTION) {
                auto x = new TApplicationException();
                x.read(iprot_);
                iprot_.getTransport().readEnd();
                throw x;
              }
              if (msg.type != TMessageType.REPLY) {
                skip(iprot_, TType.STRUCT);
                iprot_.getTransport().readEnd();
              }
              if (msg.seqid != seqid_) {
                throw new TApplicationException(
                  methodName ~ " failed: Out of sequence response.",
                  TApplicationException.Type.BAD_SEQUENCE_ID
                );
              }
              result.read(iprot_);
            });
          };

          if (methodMetaFound) {
            // DMD @@BUG@@: The if should not be necessary, but otherwise DMD ICEs
            // on empty exception arrays.
            if (!methodMeta.exceptions.empty) foreach (e; methodMeta.exceptions) {
              code ~= "if (result.isSet!`" ~ e.name ~ "`) throw result." ~
                e.name ~ ";\n";
            }
          }

          if (!is(ReturnType!(mixin("Interface." ~ methodName)) == void)) {
            code ~= q{
              if (result.isSet!`success`) return _return;
              throw new TApplicationException(
                methodName ~ " failed: Unknown result.",
                TApplicationException.Type.MISSING_RESULT
              );
            };
          }
        }
        code ~= "}\n";
      }
    }

    code ~= "}\n";
    return code;
  }());
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
      enum dToTTypeString = "TType.I32";
    } else static if (is(T _ : U[], U)) {
      enum dToTTypeString = "TType.LIST";
    } else static if (is(T _ : V[K], K, V)) {
      enum dToTTypeString = "TType.MAP";
    } else static if (is(T == struct)) {
      enum dToTTypeString = "TType.STRUCT";
    } else static if (is(T : TException)) {
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

  /**
   * Simple eager join() for strings, std.algorithm.join isn't CTFEable yet.
   */
  string ctfeJoin(string[] strings, string separator) {
    string result;
    if (strings.length > 0) {
      result ~= strings[0];
      foreach (s; strings[1..$]) {
        result ~= separator ~ s;
      }
    }
    return result;
  }
}
