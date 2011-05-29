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
import thrift.hashset;
import thrift.protocol.base;
import thrift.protocol.processor;

/**
 * Struct field requirement levels.
 */
enum TReq {
  /// The field is treated as optional when deserializing/receiving the struct
  /// and as required when serializing/sending. This is the Thrift default if
  /// neither "required" nor "optional" are specified in the IDL file.
  OPT_IN_REQ_OUT,

  /// The field is optional.
  OPTIONAL,

  /// The field is required.
  REQUIRED,

  /// Ignore the struct field when serializing/deserializing.
  IGNORE
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
          code ~= "result." ~ field.name ~ " = " ~ field.defaultValue ~ ";\n";
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

    // Returns the code string for reading a value of type F off the wire and
    // assigning it to v. The level parameter is used to make sure that there
    // are no conflicting variable names on recursive calls.
    string readValueCode(ValueType)(string v, size_t level = 0) {
      alias FullyUnqual!ValueType F;

      static if (is(F == bool)) {
        return v ~ " = p.readBool();";
      } else static if (is(F == byte)) {
        return v ~ " = p.readByte();";
      } else static if (is(F == double)) {
        return v ~ " = p.readDouble();";
      } else static if (is(F == short)) {
        return v ~ " = p.readI16();";
      } else static if (is(F == int)) {
        return v ~ " = p.readI32();";
      } else static if (is(F == long)) {
        return v ~ " = p.readI64();";
      } else static if (is(F : string)) {
        return v ~ " = p.readString();";
      } else static if (is(F == enum)) {
        return v ~ " = cast(typeof(" ~ v ~ "))p.readI32();";
      } else static if (is(F _ : E[], E)) {
        return "p.readList((TList list) {\n" ~
            // TODO: Check element type here?
            v ~ " = new typeof(" ~ v ~ "[0])[list.size];\n" ~
            "for (int i = 0; i < list.size; ++i) {\n" ~
              readValueCode!E(v ~ "[i]", level + 1) ~ "\n" ~
            "}\n" ~
          "});";
      } else static if (is(F _ : V[K], K, V)) {
        immutable key = "key" ~ to!string(level);
        immutable value = "value" ~ to!string(level);
        return "p.readMap((TMap map) {\n" ~
          v ~ " = null;\n" ~
          // TODO: Check key/value types here?
          "for (int i = 0; i < map.size; ++i) {\n" ~
            "FullyUnqual!(typeof(" ~ v ~ ".keys[0])) " ~ key ~ ";\n" ~
            readValueCode!K(key, level + 1) ~ "\n" ~
            "typeof(" ~ v ~ ".values[0]) " ~ value ~ ";\n" ~
            readValueCode!V(value, level + 1) ~ "\n" ~
            v ~ "[cast(typeof(" ~ v ~ ".keys[0]))" ~ key ~ "] = " ~ value ~ ";\n" ~
          "}\n" ~
        "});";
      } else static if (is(F _ : HashSet!(E), E)) {
        immutable elem = "elem" ~ to!string(level);
        return "p.readSet((TSet set) {\n" ~
            // TODO: Check element type here?
            v ~ " = new typeof(" ~ v ~ ")();\n" ~
            "for (int i = 0; i < set.size; ++i) {\n" ~
              "typeof(" ~ v ~ "[][0]) " ~ elem ~ ";\n" ~
              readValueCode!E(elem, level + 1) ~ "\n" ~
              v ~ " ~= " ~ elem ~ ";\n" ~
            "}\n" ~
          "});";
      } else static if (is(F == struct)) {
        return v ~ " = typeof(" ~ v ~ ")();\n" ~ v ~ ".read(p);";
      } else static if (is(F : TException)) {
        return v ~ " = new typeof(" ~ v ~ ")();\n" ~ v ~ ".read(p);";
      } else {
        static assert(false, "Cannot represent type in Thrift: " ~ F.stringof);
      }
    }

    string readFieldCode(FieldType)(string name, short id, TReq req) {
      static if (pointerStruct && isPointer!FieldType) {
        immutable v = "(*s." ~ name ~ ")";
        alias pointerTarget!FieldType F;
      } else {
        immutable v = "s." ~ name;
        alias FieldType F;
      }

      string code = "case " ~ to!string(id) ~ ":\n";
      code ~= "if (f.type == " ~ dToTTypeString!F ~ ") {\n";
      code ~= readValueCode!F(v) ~ "\n";
      if (req == TReq.REQUIRED) {
        // For required fields, set the corresponding local isSet variable.
        code ~= "isSet_" ~ name ~ " = true;\n";
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

    // The last automatically assigned id – fields with no meta information
    // are assigned (in lexical order) descending negative ids, starting with
    // -1, just like the Thrift compiler does.
    short lastId;

    foreach (name; __traits(derivedMembers, T)) {
      static if (is(MemberType!(T, name)) &&
        !isSomeFunction!(MemberType!(T, name)))
      {
        enum meta = find!`a.name == b`(fieldMetaData, name);
        static if (meta.empty) {
          --lastId;
          version (TVerboseCodegen) {
            code ~= "pragma(msg, `[thrift.codegen.readStruct] Warning: No " ~
              "meta information for field '" ~ name ~ "' in struct '" ~
              T.stringof ~ "'. Assigned id: " ~ to!string(lastId) ~ ".`);\n";
          }
          readMembersCode ~= readFieldCode!(MemberType!(T, name))(
            name, lastId, TReq.OPT_IN_REQ_OUT);
        } else static if (meta.front.req != TReq.IGNORE) {
          if (meta.front.req == TReq.REQUIRED) {
            // For required fields, generate bool flags to keep track whether
            // the field has been encountered.
            immutable n = "isSet_" ~ name;
            isSetFlagCode ~= "bool " ~ n ~ ";\n";
            isSetCheckCode ~= "enforce(" ~ n ~ ", new TProtocolException(" ~
              "`Required field '" ~ name ~ "' not found in serialized data`, " ~
              "TProtocolException.Type.INVALID_DATA));\n";
          }
          readMembersCode ~= readFieldCode!(MemberType!(T, name))(
            name, meta.front.id, meta.front.req);
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
      string writeValueCode(ValueType)(string v) {
        alias FullyUnqual!ValueType F;
        static if (is(F == bool)) {
          return "p.writeBool(" ~ v ~ ");";
        } else static if (is(F == byte)) {
          return "p.writeByte(" ~ v ~ ");";
        } else static if (is(F == double)) {
          return "p.writeDouble(" ~ v ~ ");";
        } else static if (is(F == short)) {
          return "p.writeI16(" ~ v ~ ");";
        } else static if (is(F == int)) {
          return "p.writeI32(" ~ v ~ ");";
        } else static if (is(F == long)) {
          return "p.writeI64(" ~ v ~ ");";
        } else static if (is(F : string)) {
          return "p.writeString(" ~ v ~ ");";
        } else static if (is(F == enum)) {
          return "p.writeI32(cast(int)" ~ v ~ ");";
        } else static if (is(F _ : E[], E)) {
          return "p.writeList(TList(" ~ dToTTypeString!E ~ ", " ~ v ~
            ".length), {\n" ~
            "foreach (elem; " ~ v ~ ") {\n" ~
              writeValueCode!E("elem") ~ "\n" ~
            "}\n" ~
          "});";
        } else static if (is(F _ : V[K], K, V)) {
          return "p.writeMap(TMap(" ~ dToTTypeString!K ~ ", " ~
            dToTTypeString!V ~ ", " ~ v ~ ".length), {\n" ~
            "foreach (key, value; " ~ v ~ ") {\n" ~
              writeValueCode!K("key") ~ "\n" ~
              writeValueCode!V("value") ~ "\n" ~
            "}\n" ~
          "});";
        } else static if (is(F _ : HashSet!E, E)) {
          return "p.writeSet(TSet(" ~ dToTTypeString!E ~ ", " ~ v ~
            ".length), {\n" ~
            "foreach (elem; " ~ v ~ ") {\n" ~
              writeValueCode!E("elem") ~ "\n" ~
            "}\n" ~
          "});";
        } else static if (is(F == struct)) {
          return v ~ ".write(p);";
        } else static if (is(F : TException)) {
          return v ~ ".write(p);";
        } else {
          static assert(false, "Cannot represent type in Thrift: " ~ F.stringof);
        }
      }

      string writeFieldCode(FieldType)(string name, short id, TReq req) {
        string code;
        if (!pointerStruct && req == TReq.OPTIONAL) {
          code ~= "if (s.isSet!`" ~ name ~ "`()) {\n";
        }

        static if (pointerStruct && isPointer!FieldType) {
          immutable v = "(*s." ~ name ~ ")";
          alias pointerTarget!FieldType F;
        } else {
          immutable v = "s." ~ name;
          alias FieldType F;
        }

        code ~= "p.writeField(TField(`" ~ name ~ "`, " ~ dToTTypeString!F ~
          ", " ~ to!string(id) ~ "), { " ~ writeValueCode!F(v) ~ " });\n";

        if (!pointerStruct && req == TReq.OPTIONAL) {
          code ~= "}\n";
        }
        return code;
      }

      // The last automatically assigned id – fields with no meta information
      // are assigned (in lexical order) descending negative ids, starting with
      // -1, just like the Thrift compiler does.
      short lastId;

      string code = "";
      foreach (name; __traits(derivedMembers, T)) {
        static if (is(MemberType!(T, name)) &&
          !isSomeFunction!(MemberType!(T, name)))
        {
          alias MemberType!(T, name) F;

          auto meta = find!`a.name == b`(fieldMetaData, name);
          if (meta.empty) {
            --lastId;
            version (TVerboseCodegen) {
              code ~= "pragma(msg, `[thrift.codegen.writeStruct] Warning: No " ~
                "meta information for field '" ~ name ~ "' in struct '" ~
                T.stringof ~ "'. Assigned id: " ~ to!string(lastId) ~ ".`);\n";
            }
            code ~= writeFieldCode!F(name, lastId, TReq.OPT_IN_REQ_OUT);
          } else if (meta.front.req != TReq.IGNORE) {
            code ~= writeFieldCode!F(name, meta.front.id, meta.front.req);
          }
        }
      }

      return code;
    }());
  });
}

template TArgsStruct(Interface, string methodName) {
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

      memberCode ~= "ParameterTypeTuple!(Interface." ~ methodName ~
        ")[" ~ to!string(i) ~ "]" ~ memberName ~ ";\n";

      fieldMetaCodes ~= "TFieldMeta(`" ~ memberName ~ "`, " ~
        to!string(methodMetaFound ? methodMeta.params[i].id : (i + 1)) ~ ")";
    }

    string code = "struct TArgsStruct {\n";
    code ~= memberCode;
    version (TVerboseCodegen) {
      if (!methodMetaFound &&
        ParameterTypeTuple!(mixin("Interface." ~ methodName)).length > 0)
      {
        code ~= "pragma(msg, `[thrift.codegen.TArgsStruct] Warning: No " ~
          "meta information for method '" ~ methodName ~ "' in service '" ~
          Interface.stringof ~ "' found.`);\n";
      }
    }
    immutable fieldMetaCode =
      fieldMetaCodes.empty ? "" : "[" ~ ctfeJoin(fieldMetaCodes) ~ "]";
    code ~= "mixin TStructHelpers!(" ~ fieldMetaCode  ~ ");\n";
    code ~= "}\n";
    return code;
  }());
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
    version (TVerboseCodegen) {
      if (!methodMetaFound &&
        ParameterTypeTuple!(mixin("Interface." ~ methodName)).length > 0)
      {
        code ~= "pragma(msg, `[thrift.codegen.TPargsStruct] Warning: No " ~
          "meta information for method '" ~ methodName ~ "' in service '" ~
          Interface.stringof ~ "' found.`);\n";
      }
    }
    code ~= "void write(TProtocol proto) const {\n";
    code ~= "writeStruct!(TPargsStruct, [" ~ ctfeJoin(fieldMetaCodes) ~
      "], true)(this, proto);\n";
    code ~= "}\n";
    code ~= "}\n";
    return code;
  }());
}

template TResultStruct(Interface, string methodName) {
  static assert(is(typeof(mixin("Interface." ~ methodName))),
    "Could not find method '" ~ methodName ~ "' in '" ~ Interface.stringof ~ "'.");

  mixin({
    string code = "struct TResultStruct {\n";

    string[] fieldMetaCodes;

    static if (!is(ReturnType!(mixin("Interface." ~ methodName)) == void)) {
      code ~= "ReturnType!(Interface." ~ methodName ~ ") success;\n";
      fieldMetaCodes ~= "TFieldMeta(`success`, 0, TReq.OPTIONAL)";
    }

    bool methodMetaFound;
    static if (is(typeof(Interface.methodMeta) : TMethodMeta[])) {
      auto meta = find!`a.name == b`(Interface.methodMeta, methodName);
      if (!meta.empty) {
        // DMD @@BUG@@: The if should not be necessary, but otherwise DMD ICEs
        // on empty exception arrays.
        if (!meta.front.exceptions.empty) foreach (e; meta.front.exceptions) {
          code ~= "Interface." ~ e.type ~ " " ~ e.name ~ ";\n";
          fieldMetaCodes ~= "TFieldMeta(`" ~ e.name ~ "`, " ~ to!string(e.id) ~
            ", TReq.OPTIONAL)";
        }
        methodMetaFound = true;
      }
    }

    version (TVerboseCodegen) {
      if (!methodMetaFound &&
        ParameterTypeTuple!(mixin("Interface." ~ methodName)).length > 0)
      {
        code ~= "pragma(msg, `[thrift.codegen.TResultStruct] Warning: No " ~
          "meta information for method '" ~ methodName ~ "' in service '" ~
          Interface.stringof ~ "' found.`);\n";
      }
    }

    immutable fieldMetaCode =
      fieldMetaCodes.empty ? "" : "[" ~ ctfeJoin(fieldMetaCodes) ~ "]";
    code ~= "mixin TStructHelpers!(" ~ fieldMetaCode  ~ ");\n";
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

    alias ReturnType!(mixin("Interface." ~ methodName)) ResultType;
    static if (!is(ResultType == void)) {
      code ~= q{
        ReturnType!(mixin("Interface." ~ methodName))* success;
      };
      fieldMetaCodes ~= "TFieldMeta(`success`, 0, TReq.OPTIONAL)";

      static if (!isNullable!ResultType) {
        code ~= q{
          struct IsSetFlags {
            bool success;
          }
          IsSetFlags isSetFlags;
        };
        fieldMetaCodes ~= "TFieldMeta(`isSetFlags`, 0, TReq.IGNORE)";
      }
    }

    bool methodMetaFound;
    static if (is(typeof(Interface.methodMeta) : TMethodMeta[])) {
      auto meta = find!`a.name == b`(Interface.methodMeta, methodName);
      if (!meta.empty) {
        // DMD @@BUG@@: The if should not be necessary, but otherwise DMD ICEs
        // on empty exception arrays.
        if (!meta.front.exceptions.empty) foreach (e; meta.front.exceptions) {
          code ~= "Interface." ~ e.type ~ " " ~ e.name ~ ";\n";
          fieldMetaCodes ~= "TFieldMeta(`" ~ e.name ~ "`, " ~ to!string(e.id) ~
            ", TReq.OPTIONAL)";
        }
        methodMetaFound = true;
      }
    }

    version (TVerboseCodegen) {
      if (!methodMetaFound &&
        ParameterTypeTuple!(mixin("Interface." ~ methodName)).length > 0)
      {
        code ~= "pragma(msg, `[thrift.codegen.TPresultStruct] Warning: No " ~
          "meta information for method '" ~ methodName ~ "' in service '" ~
          Interface.stringof ~ "' found.`);\n";
      }
    }

    code ~= q{
      bool isSet(string fieldName)() const if (is(MemberType!(typeof(this), fieldName))) {
        static if (fieldName == "success") {
          static if (isNullable!(typeof(*success))) {
            return *success !is null;
          } else {
            return isSetFlags.success;
          }
        } else {
          // We are dealing with an exception member, which, being a nullable
          // type (exceptions are always classes), has no isSet flag.
          return __traits(getMember, this, fieldName) !is null;
        }
      }
    };

    code ~= "void read(TProtocol proto) {\n";
    code ~= "readStruct!(TPresultStruct, [" ~ ctfeJoin(fieldMetaCodes) ~
      "], true)(this, proto);\n";
    code ~= "}\n";
    code ~= "}\n";
    return code;
  }());
}

template TClient(Interface) if (is(Interface _ == interface)) {
  mixin({
    static if (is(Interface BaseInterfaces == super) && BaseInterfaces.length > 0) {
      static assert(BaseInterfaces.length == 1,
        "Services cannot be derived from more than one parent.");

      string code =
        "class TClient : TClient!(BaseTypeTuple!(Interface)[0]), Interface {\n";
      code ~= q{
        this(TProtocol iprot, TProtocol oprot) {
          super(iprot, oprot);
        }

        this(TProtocol prot) {
          super(prot);
        }
      };
    } else {
      string code = "class TClient : Interface {";
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
          "(" ~ ctfeJoin(paramList) ~ ") {\n";

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
                auto x = new TApplicationException(null);
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

template TServiceProcessor(Interface) if (is(Interface _ == interface)) {
  mixin({
    static if (is(Interface BaseInterfaces == super) && BaseInterfaces.length > 0) {
      static assert(BaseInterfaces.length == 1,
        "Services cannot be derived from more than one parent.");

      string code = "class TServiceProcessor : " ~
        "TServiceProcessor!(BaseTypeTuple!(Interface)[0]) {\n";
      code ~= "private Interface iface_;\n";

      string constructorCode = "this(Interface iface) {\n";
      constructorCode ~= "super(iface);\n";
      constructorCode ~= "iface_ = iface;\n";
    } else {
      string code = "class TServiceProcessor : TProcessor {";
      code ~= q{
        override bool process(TProtocol iprot, TProtocol oprot) {
          iprot.readMessage((TMessage msg) {
            if (msg.type != TMessageType.CALL && msg.type != TMessageType.ONEWAY) {
              skip(iprot, TType.STRUCT);
              auto x = new TApplicationException(
                TApplicationException.Type.INVALID_MESSAGE_TYPE);
              oprot.writeMessage(TMessage(msg.name, TMessageType.EXCEPTION,
                msg.seqid), { x.write(oprot); });
              oprot.getTransport().writeEnd();
              oprot.getTransport().flush();
            } else if (auto dg = msg.name in processMap_) {
              (*dg)(msg.seqid, iprot, oprot);
            } else {
              skip(iprot, TType.STRUCT);
              auto x = new TApplicationException("Invalid method name: '" ~
                msg.name ~ "'.", TApplicationException.Type.INVALID_MESSAGE_TYPE);
              oprot.writeMessage(TMessage(msg.name, TMessageType.EXCEPTION,
                msg.seqid), { x.write(oprot); });
              oprot.getTransport().writeEnd();
              oprot.getTransport().flush();
            }

          });
          iprot.getTransport().readEnd();
          return true;
        }

        alias void delegate(int, TProtocol, TProtocol) ProcessFunc;
        protected ProcessFunc[string] processMap_;
        private Interface iface_;
      };

      string constructorCode = "this(Interface iface) {\n";
      constructorCode ~= "iface_ = iface;\n";
    }

    foreach (methodName; __traits(derivedMembers, Interface)) {
      static if (isSomeFunction!(mixin("Interface." ~ methodName))) {
        immutable procFuncName = "process_" ~ methodName;
        constructorCode ~= "processMap_[`" ~ methodName ~ "`] = &" ~
          procFuncName ~ ";\n";

        bool methodMetaFound;
        TMethodMeta methodMeta;
        static if (is(typeof(Interface.methodMeta) : TMethodMeta[])) {
          enum meta = find!`a.name == b`(Interface.methodMeta, methodName);
          if (!meta.empty) {
            methodMetaFound = true;
            methodMeta = meta.front;
          }
        }

        code ~= "void " ~ procFuncName ~ "(int seqid, TProtocol iprot, TProtocol oprot) {\n";
        code ~= "TArgsStruct!(Interface, `" ~ methodName ~ "`) args;\n";
        code ~= "args.read(iprot);\n";

        code ~= "TResultStruct!(Interface, `" ~ methodName ~ "`) result;\n";
        code ~= "try {\n";

        // Generate the parameter list to pass to the called iface function.
        string[] paramList;
        foreach (i, _; ParameterTypeTuple!(mixin("Interface." ~ methodName))) {
          paramList ~= "args." ~ (methodMetaFound ? methodMeta.params[i].name :
              "param" ~ to!string(i + 1));
        }

        immutable call = "iface_." ~ methodName ~ "(" ~ ctfeJoin(paramList) ~ ")";
        if (is(ReturnType!(mixin("Interface." ~ methodName)) == void)) {
          code ~= call ~ ";\n";
        } else {
          code ~= "result.set!`success`(" ~ call ~ ");\n";
        }

        // If this is not a oneway method, generate the recieving code.
        if (!methodMetaFound || methodMeta.type != TMethodType.ONEWAY) {
          // DMD @@BUG@@: The second if condition should not be necessary, but
          // otherwise DMD ICEs on empty exception arrays.
          if (methodMetaFound && !methodMeta.exceptions.empty) {
            foreach (e; methodMeta.exceptions) {
              code ~= "} catch (Interface." ~ e.type ~ " " ~ e.name ~ ") {\n";
              code ~= "result.set!`" ~ e.name ~ "`(" ~ e.name ~ ");\n";
            }
          }

          code ~= "} catch (Exception e) {\n";
          code ~= "auto x = new TApplicationException(to!string(e));\n";
          code ~= "oprot.writeMessage(TMessage(`" ~ methodName ~ "`, " ~
            "TMessageType.EXCEPTION, seqid), { x.write(oprot); });\n";
          code ~= "oprot.getTransport().writeEnd();\n";
          code ~= "oprot.getTransport().flush();\n";
          code ~= "return;\n";
          code ~= "}\n";

          code ~= "oprot.writeMessage(TMessage(`" ~ methodName ~ "`, " ~
            "TMessageType.REPLY, seqid), { result.write(oprot); });\n";
          code ~= "oprot.getTransport().writeEnd();\n";
          code ~= "oprot.getTransport().flush();\n";
        } else {
          // There is nothing really what we can do about unhandled exceptions
          // in oneway methods as long as event handlers are not implemented –
          // for now, just pass them on.
          code ~= "} catch (Exception e) {\n";
          code ~= "throw e;\n";
          code ~= "}\n";
        }
        code ~= "}\n";
      }
    }

    code ~= constructorCode ~ "}\n";
    code ~= "}\n";

    return code;
  }());
}

/**
 * Removes all type qualifiers from T.
 *
 * In contrast to std.traits.Unqual, FullyUnqual also removes qualifiers from
 * array elements (e.g. immutable(byte[]) -> byte[], not immutable(byte)[]),
 * excluding strings (string isn't reduced to char[]).
 */
template FullyUnqual(T) {
  static if (is(T _ == const(U), U)) {
    alias FullyUnqual!U FullyUnqual;
  } else static if (is(T _ == immutable(U), U)) {
    alias FullyUnqual!U FullyUnqual;
  } else static if (is(T _ == shared(U), U)) {
    alias FullyUnqual!U FullyUnqual;
  } else static if (is(T _ == U[], U) && !isSomeString!T) {
    alias FullyUnqual!(U)[] FullyUnqual;
  } else static if (is(T _ == V[K], K, V)) {
    alias FullyUnqual!(V)[FullyUnqual!K] FullyUnqual;
  } else {
    alias T FullyUnqual;
  }
}

private {
  /**
   * Returns a D code string containing the matching TType value for a passed
   * D type, e.g. dToTTypeString!byte == "TType.BYTE".
   */
  template dToTTypeString(T) {
    static if (is(FullyUnqual!T == bool)) {
      enum dToTTypeString = "TType.BOOL";
    } else static if (is(FullyUnqual!T == byte)) {
      enum dToTTypeString = "TType.BYTE";
    } else static if (is(FullyUnqual!T == double)) {
      enum dToTTypeString = "TType.DOUBLE";
    } else static if (is(FullyUnqual!T == short)) {
      enum dToTTypeString = "TType.I16";
    } else static if (is(FullyUnqual!T == int)) {
      enum dToTTypeString = "TType.I32";
    } else static if (is(FullyUnqual!T == long)) {
      enum dToTTypeString = "TType.I64";
    } else static if (is(FullyUnqual!T : string)) {
      enum dToTTypeString = "TType.STRING";
    } else static if (is(FullyUnqual!T == enum)) {
      enum dToTTypeString = "TType.I32";
    } else static if (is(FullyUnqual!T _ : U[], U)) {
      enum dToTTypeString = "TType.LIST";
    } else static if (is(FullyUnqual!T _ : V[K], K, V)) {
      enum dToTTypeString = "TType.MAP";
    } else static if (is(FullyUnqual!T _ : HashSet!E, E)) {
      enum dToTTypeString = "TType.SET";
    } else static if (is(FullyUnqual!T == struct)) {
      enum dToTTypeString = "TType.STRUCT";
    } else static if (is(FullyUnqual!T : TException)) {
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
  string ctfeJoin(string[] strings, string separator = ", ") {
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
