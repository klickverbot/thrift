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
 * Code generation templates used for implementing struct serialization,
 * service processors and clients.
 *
 * Typically, only the metadata types, TClient, TServiceProcessor and
 * TStructHelpers are used by client code, the other templates are mainly for
 * internal use. For a simple usage example, you might want to have a look at
 * the D tutorial implementation in the top-level tutorial/ directory.
 *
 * Several artifacts in this module have options for specifying the exact
 * TProtocol type used. If it is, the amount of needed virtual calls can be
 * reduced and as a result, the code also can be optimized better. If
 * performance is not a concern or the actual protocol type is not known at
 * compile time, these parameters can just be left at their defaults.
 *
 * Some code generation templates take account of the optional TVerboseCodegen
 * version declaration, which causes warning messages to be emitted if no
 * metadata for a field/method has been found and the default behavior is
 * used instead. If this version is not defined, the templates just silently
 * behave like the Thrift compiler does in this situation, i.e. automatically
 * assign negative ids (starting at -1) for fields and assume
 * TReq.OPT_IN_REQ_OUT as requirement level.
 */
module thrift.codegen;

import core.time : Duration, TickDuration;
import std.algorithm : find, max, map, min, reduce, remove;
import std.array : empty, front;
import std.conv : to;
import std.exception : enforce;
import std.random : randomCover, rndGen;
import std.range;
import std.traits;
import std.typetuple : allSatisfy, TypeTuple;
import std.variant : Variant;
import thrift.async.base;
import thrift.base;
import thrift.hashset;
import thrift.protocol.base;
import thrift.protocol.processor;
import thrift.transport.base;

/*
 * Thrift struct/service meta data, which is used to store information from
 * the interface definition files not representable in plain D, i.e. field
 * requirement levels, Thrift field IDs, etc.
 */

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

/**
 * The way how methods are called.
 */
enum TMethodType {
  /// Called in the normal two-way scheme consisting of a request and a
  /// response.
  REGULAR,

  /// A fire-and-forget one-way method, where no response is sent and the
  /// client immediately returns.
  ONEWAY
}

/**
 * Compile-time metadata for a struct field.
 */
struct TFieldMeta {
  /// The name of the field. Used for matching a TFieldMeta with the actual
  /// D struct member during code generation.
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
  /// The name of the method. Used for matching a TMethodMeta with the actual
  /// method during code generation.
  string name;

  /// Meta information for the parameteres.
  TParamMeta[] params;

  /// Specifies which exceptions can be thrown by the method. All other
  /// exceptions are converted to a TApplicationException instead.
  TExceptionMeta[] exceptions;

  /// The fundamental type of the method.
  TMethodType type;
}

/**
 * Compile-time metadata for a service method parameter.
 */
struct TParamMeta {
  /// The name of the parameter. Contrary to TFieldMeta, it only serves
  /// decorative purposes here.
  string name;

  /// The Thrift id of the parameter in the param struct.
  short id;

  /// A code string containing a D expression for the default value for the
  /// parameter, if any.
  string defaultValue;
}

/**
 * Compile-time metadata for a service method exception annotation.
 */
struct TExceptionMeta {
  /// The name of the exception »return value«. Contrary to TFieldMeta, it
  /// only serves decorative purposes here, as it is only used in code not
  /// visible to processor implementations/service clients.
  string name;

  /// The Thrift id of the exception field in the return value struct.
  short id;

  /// The name of the exception type.
  string type;
}


/*
 * Code generation templates.
 */

/**
 * Mixin template defining additional helper methods for using a struct with
 * Thrift, and a member called isSetFlags if the struct contains any fields
 * for which an »is set« flag is needed.
 *
 * It can only be used inside structs or Exception classes.
 *
 * For example, consider the following struct definition:
 * ---
 * struct Foo {
 *   string a;
 *   int b;
 *   int c;
 *
 *   mixin TStructHelpers!([
 *     TFieldMeta("a", 1),
 *     TFieldMeta("b", 2),
 *     TFieldMeta("c", 3, TReq.REQUIRED, "4")
 *   ]);
 * }
 * ---
 *
 * TStructHelper adds the following methods to the struct:
 * ---
 * /++
 *  + Sets member fieldName to the given value and marks it as set.
 *  +
 *  + Examples:
 *  + ---
 *  + auto f = Foo();
 *  + f.set!"b"(12345);
 *  + assert(f.isSet!"b");
 *  + ---
 *  +/
 * void set(string fieldName)(MemberType!(This, fieldName) value);
 *
 * /++
 *  + Resets member fieldName to the init property of its type and marks it as
 *  + not set.
 *  +
 *  + Examples:
 *  + ---
 *  + // Set f.b to some value.
 *  + auto f = Foo();
 *  + f.set!"b"(12345);
 *  +
 *  + f.unset!b();
 *  +
 *  + // f.b is now unset again.
 *  + assert(!f.isSet!"b");
 *  + ---
 *  +/
 * void unset(string fieldName);
 *
 * /++
 *  + Returns whether member fieldName is set.
 *  +
 *  + Examples:
 *  + ---
 *  + auto f = Foo();
 *  + assert(!f.isSet!"b");
 *  + f.set!"b"(12345);
 *  + assert(f.isSet!"b");
 *  + ---
 *  +/
 * bool isSet(string fieldName)() const @property;
 *
 * /++
 *  + Returns a string representation of the struct.
 *  +
 *  + Examples:
 *  + ---
 *  + auto f = Foo();
 *  + f.a = "a string";
 *  + assert(f.toString() == `Foo("a string", 0 (unset), 4)`);
 *  + ---
 *  +/
 * string toString() const;
 *
 * /++
 *  + Deserializes the struct, setting its members to the values read from the
 *  + protocol. Forwards to readStruct(this, proto);
 *  +/
 * void read(Protocol)(Protocol proto) if (isTProtocol!Protocol);
 *
 * /++
 *  + Serializes the struct to the target protocol. Forwards to
 *  + writeStruct(this, proto);
 *  +/
 * void write(Protocol)(Protocol proto) const if (isTProtocol!Protocol);
 * ---
 *
 * Additionally, an opEquals() implementation is provided which simply
 * compares all fields, but disregards the is set struct, if any (the exact
 * signature obviously differs between structs and exception classes).
 *
 * Note: To set the default values for fields where one has been specified in
 * the field metadata, a parameterless static opCall is generated, because D
 * does not allow parameterless (default) constructors for structs. Thus, be
 * always to use to initialize structs:
 * ---
 * Foo foo; // Wrong!
 * auto foo = Foo(); // Correct.
 * ---
 */
mixin template TStructHelpers(alias fieldMetaData = cast(TFieldMeta[])null) if (
  is(typeof(fieldMetaData) : TFieldMeta[])
) {
  import std.algorithm : canFind;
  import thrift.protocol.base : TProtocol;

  alias typeof(this) This;
  static assert(is(This == struct) || is(This : Exception),
    "TStructHelpers can only be used inside a struct or an Exception class.");

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
    if (is(typeof(mixin("this.isSetFlags." ~ fieldName)) : bool)) {
      __traits(getMember, this.isSetFlags, fieldName) = false;
    }
    __traits(getMember, this, fieldName) = MemberType!(This, fieldName).init;
  }

  bool isSet(string fieldName)() const @property if (
    is(MemberType!(This, fieldName))
  ) {
    static if (isNullable!(MemberType!(This, fieldName))) {
      return __traits(getMember, this, fieldName) !is null;
    } else static if (is(typeof(mixin("this.isSetFlags." ~ fieldName)) : bool)) {
      return __traits(getMember, this.isSetFlags, fieldName);
    } else {
      // This is a required field, which is always set.
      return true;
    }
  }

  static if (is(This _ == class)) {
    override string toString() const {
      return thriftToStringImpl();
    }

    override bool opEquals(Object other) const {
      auto rhs = cast(This)other;
      if (rhs) {
        return thriftOpEqualsImpl(rhs);
      }

      return super.opEquals(other);
    }
  } else {
    string toString() const {
      return thriftToStringImpl();
    }

    bool opEquals(ref const This other) const {
      return thriftOpEqualsImpl(other);
    }
  }

  private string thriftToStringImpl() const {
    string result = This.stringof ~ "(";
    mixin({
      string code = "";
      bool first = true;
      foreach (i, name; __traits(derivedMembers, This)) {
        static if (!is(MemberType!(This, name))) {
          // We hit something strange like the TStructHelpers template itself,
          // just ignore.
        } else {
          import std.traits;
          static if (isCallable!(MemberType!(This, name))) {
            // We don't want to pick up e.g. the __ctor member for exceptions.
            // Cannot use continue here due to »unreachable statement« warnings.
          } else {
            if (first) {
              first = false;
            } else {
              code ~= "result ~= `, `;\n";
            }
            code ~= "result ~= `" ~ name ~ ": ` ~ to!string(this." ~ name ~ ");\n";
            code ~= "if (!isSet!q{" ~ name ~ "}) {\n";
            code ~= "result ~= ` (unset)`;\n";
            code ~= "}\n";
          }
        }
      }
      return code;
    }());
    result ~= ")";
    return result;
  }

  private bool thriftOpEqualsImpl(const ref This rhs) const {
    foreach (i, name; __traits(derivedMembers, This)) {
      static if (!is(MemberType!(This, name))) {
        // We hit something strange like the TStructHelpers template itself,
        // just ignore.
      } else {
        if (mixin("this." ~ name) != mixin("rhs." ~ name)) return false;
      }
    }
    return true;
  }

  static if (canFind!`!a.defaultValue.empty`(fieldMetaData)) {
    static if (is(This _ == class)) {
      this() {
        mixin(thriftFieldInitCode("this"));
      }
    } else {
      // DMD @@BUG@@: Have to use auto here to avoid »no size yet for forward
      // reference« errors.
      static auto opCall() {
        auto result = This.init;
        mixin(thriftFieldInitCode("result"));
        return result;
      }
    }

    private static string thriftFieldInitCode(string thisName) {
      string code;
      foreach (field; fieldMetaData) {
        if (field.defaultValue.empty) continue;
        code ~= thisName ~ "." ~ field.name ~ " = " ~ field.defaultValue ~ ";\n";
      }
      return code;
    }
  }

  void read(Protocol)(Protocol proto) if (isTProtocol!Protocol) {
    readStruct!(This, Protocol, fieldMetaData, false)(this, proto);
  }

  void write(Protocol)(Protocol proto) const if (isTProtocol!Protocol) {
    writeStruct!(This, Protocol, fieldMetaData, false)(this, proto);
  }
}

version (unittest) {
  // Cannot make this nested in the unittest block due to a »no size yet for
  // forward reference« error.
  struct Foo {
    string a;
    int b;
    int c;

    mixin TStructHelpers!([
      TFieldMeta("a", 1),
      TFieldMeta("b", 2),
      TFieldMeta("c", 3, TReq.REQUIRED, "4")
    ]);
  }
}
unittest {
  auto f = Foo();

  f.set!"b"(12345);
  assert(f.isSet!"b");
  f.unset!"b"();
  assert(!f.isSet!"b");
  f.set!"b"(12345);
  assert(f.isSet!"b");
  f.unset!"b"();

  f.a = "a string";
  assert(f.toString() == `Foo(a: a string, b: 0 (unset), c: 4)`);
}


/**
 * Generates an eponymous struct with boolean flags for the non-required
 * non-nullable fields of T, if any, or nothing otherwise (i.e. the template
 * body is empty).
 *
 * Nullable fields are just set to null to signal »not set«.
 *
 * In most cases, you do not want to use this directly, but via TStructHelpers
 * instead.
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
 * Deserializes a Thrift struct from a protocol.
 *
 * Using the Protocol template parameter, the concrete TProtocol to use can be
 * be specified. If the pointerStruct parameter is set to true, the struct
 * fields are expected to be pointers to the actual data. This is used
 * internally (combined with TPResultStruct) and usually should not be used in
 * user code.
 *
 * This is a free function to make it possible to read exisiting structs from
 * the wire without altering their definitions.
 */
void readStruct(T, Protocol, alias fieldMetaData = cast(TFieldMeta[])null,
  bool pointerStruct = false)(ref T s, Protocol p) if (isTProtocol!Protocol)
{
  mixin({
    string code;

    // Check that all fields for which there is meta info are actually in the
    // passed struct type.
    static if (fieldMetaData) foreach (field; fieldMetaData) {
      code ~= "static assert(is(MemberType!(T, `" ~ field.name ~ "`)));\n";
    }

    // Returns the code string for reading a value of type F off the wire and
    // assigning it to v. The level parameter is used to make sure that there
    // are no conflicting variable names on recursive calls.
    string readValueCode(ValueType)(string v, size_t level = 0) {
      // Some non-ambigous names to use (shadowing is not allowed in D).
      immutable i = "i" ~ to!string(level);
      immutable elem = "elem" ~ to!string(level);
      immutable key = "key" ~ to!string(level);
      immutable list = "list" ~ to!string(level);
      immutable map = "map" ~ to!string(level);
      immutable set = "set" ~ to!string(level);
      immutable value = "value" ~ to!string(level);

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
        return "{\n" ~
          "auto " ~ list ~ " = p.readListBegin();\n" ~
          // TODO: Check element type here?
          v ~ " = new typeof(" ~ v ~ "[0])[" ~ list ~ ".size];\n" ~
          "foreach (" ~ i ~ "; 0 .. " ~ list ~ ".size) {\n" ~
            readValueCode!E(v ~ "[" ~ i ~ "]", level + 1) ~ "\n" ~
          "}\n" ~
          "p.readListEnd();\n" ~
        "}";
      } else static if (is(F _ : V[K], K, V)) {
        return "{\n" ~
          "auto " ~ map ~ " = p.readMapBegin();" ~
          v ~ " = null;\n" ~
          // TODO: Check key/value types here?
          "foreach (" ~ i ~ "; 0 .. " ~ map ~ ".size) {\n" ~
            "FullyUnqual!(typeof(" ~ v ~ ".keys[0])) " ~ key ~ ";\n" ~
            readValueCode!K(key, level + 1) ~ "\n" ~
            "typeof(" ~ v ~ ".values[0]) " ~ value ~ ";\n" ~
            readValueCode!V(value, level + 1) ~ "\n" ~
            v ~ "[cast(typeof(" ~ v ~ ".keys[0]))" ~ key ~ "] = " ~ value ~ ";\n" ~
          "}\n" ~
          "p.readMapEnd();" ~
        "}";
      } else static if (is(F _ : HashSet!(E), E)) {
        return "{\n" ~
          "auto " ~ set ~ " = p.readSetBegin();" ~
          // TODO: Check element type here?
          v ~ " = new typeof(" ~ v ~ ")();\n" ~
          "foreach (" ~ i ~ "; 0 .. " ~ set ~ ".size) {\n" ~
            "typeof(" ~ v ~ "[][0]) " ~ elem ~ ";\n" ~
            readValueCode!E(elem, level + 1) ~ "\n" ~
            v ~ " ~= " ~ elem ~ ";\n" ~
          "}\n" ~
          "p.readSetEnd();" ~
        "}";
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
    code ~= "p.readStructBegin();\n";
    code ~= "while (true) {\n";
    code ~= "auto f = p.readFieldBegin();\n";
    code ~= "if (f.type == TType.STOP) break;\n";
    code ~= "switch(f.id) {\n";
    code ~= readMembersCode;
    code ~= "default: skip(p, f.type);\n";
    code ~= "}\n";
    code ~= "p.readFieldEnd();\n";
    code ~= "}\n";
    code ~= "p.readStructEnd();\n";
    code ~= isSetCheckCode;

    return code;
  }());
}

/**
 * Serializes a struct to the target protocol.
 *
 * Using the Protocol template parameter, the concrete TProtocol to use can be
 * be specified. If the pointerStruct parameter is set to true, the struct
 * fields are expected to be pointers to the actual data. This is used
 * internally (combined with TPargsStruct) and usually should not be used in
 * user code.
 *
 * This is a free function to make it possible to read exisiting structs from
 * the wire without altering their definitions.
 */
void writeStruct(T, Protocol, alias fieldMetaData = cast(TFieldMeta[])null,
  bool pointerStruct = false) (const T s, Protocol p) if (isTProtocol!Protocol)
{
  mixin({
    // Check that all fields for which there is meta info are actually in the
    // passed struct type.
    string code = "";
    static if (fieldMetaData) foreach (field; fieldMetaData) {
      code ~= "static assert(is(MemberType!(T, `" ~ field.name ~ "`)));\n";
    }

    // Check that required nullable members are non-null.
    // WORKAROUND: To stop LDC from emitting the manifest constant »meta« below
    // into the writeStruct function body this is inside the string mixin
    // block – the code wouldn't depend on it (this is an LDC bug, and because
    // of it a new array would be allocate on each method invocation at runtime).
    foreach (name; __traits(derivedMembers, T)) {
      static if (is(MemberType!(T, name)) &&
        !isSomeFunction!(MemberType!(T, name)))
      {
        static if (isNullable!(MemberType!(T, name))) {
          enum meta = find!`a.name == b`(fieldMetaData, name);
          static if (!meta.empty && meta.front.req == TReq.REQUIRED) {
            code ~= `enforce(__traits(getMember, s, name) !is null,
              new TException("Required field '` ~ name ~ `' is null."));\n`;
          }
        }
      }
    }

    return code;
  }());

  p.writeStructBegin(TStruct(T.stringof));
  mixin({
    string writeValueCode(ValueType)(string v, size_t level = 0) {
      // Some non-ambigous names to use (shadowing is not allowed in D).
      immutable elem = "elem" ~ to!string(level);
      immutable key = "key" ~ to!string(level);
      immutable value = "value" ~ to!string(level);

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
        return "p.writeListBegin(TList(" ~ dToTTypeString!E ~ ", " ~ v ~
          ".length));\n" ~
          "foreach (" ~ elem ~ "; " ~ v ~ ") {\n" ~
            writeValueCode!E(elem, level + 1) ~ "\n" ~
          "}\n" ~
          "p.writeListEnd();";
      } else static if (is(F _ : V[K], K, V)) {
        return "p.writeMapBegin(TMap(" ~ dToTTypeString!K ~ ", " ~
          dToTTypeString!V ~ ", " ~ v ~ ".length));\n" ~
          "foreach (" ~ key ~ ", " ~ value ~ "; " ~ v ~ ") {\n" ~
            writeValueCode!K(key, level + 1) ~ "\n" ~
            writeValueCode!V(value, level + 1) ~ "\n" ~
          "}\n" ~
          "p.writeMapEnd();";
      } else static if (is(F _ : HashSet!E, E)) {
        return "p.writeSetBegin(TSet(" ~ dToTTypeString!E ~ ", " ~ v ~
          ".length));\n" ~
          "foreach (" ~ elem ~ "; " ~ v ~ ") {\n" ~
            writeValueCode!E(elem, level + 1) ~ "\n" ~
          "}\n" ~
          "p.writeSetEnd();";
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
        code ~= "if (s.isSet!`" ~ name ~ "`) {\n";
      }

      static if (pointerStruct && isPointer!FieldType) {
        immutable v = "(*s." ~ name ~ ")";
        alias pointerTarget!FieldType F;
      } else {
        immutable v = "s." ~ name;
        alias FieldType F;
      }

      code ~= "p.writeFieldBegin(TField(`" ~ name ~ "`, " ~ dToTTypeString!F ~
        ", " ~ to!string(id) ~ "));\n";
      code ~= writeValueCode!F(v) ~ "\n";
      code ~= "p.writeFieldEnd();\n";

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
  p.writeFieldStop();
  p.writeStructEnd();
}

/**
 * A struct representing the arguments of a Thrift method call.
 *
 * Consider this example:
 * ---
 * interface Foo {
 *   int bar(string a, bool b);
 *
 *   enum methodMeta = [
 *     TMethodMeta("bar", [TParamMeta("a", 1), TParamMeta("b", 2)])
 *   ];
 * }
 *
 * alias TArgsStruct!Foo FooBarArgs;
 * ---
 *
 * The definition of FooBarArgs is equivalent to:
 * ---
 * struct FooBarArgs {
 *   string a;
 *   bool b;
 *
 *   mixin TStructHelpers!([TFieldMeta("a", 1), TFieldMeta("b", 2)]);
 * }
 * ---
 *
 * If the TVerboseCodegen version is defined, a warning message is issued at
 * compilation if no TMethodMeta for Interface.methodName is found.
 */
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
      string memberId;
      string memberName;
      if (methodMetaFound && i < methodMeta.params.length) {
        memberId = to!string(methodMeta.params[i].id);
        memberName = methodMeta.params[i].name;
      } else {
        memberId = to!string(i + 1);
        memberName = "param" ~ to!string(i + 1);
      }

      memberCode ~= "ParameterTypeTuple!(Interface." ~ methodName ~
        ")[" ~ to!string(i) ~ "]" ~ memberName ~ ";\n";

      fieldMetaCodes ~= "TFieldMeta(`" ~ memberName ~ "`, " ~ memberId ~ ")";
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

/**
 * Like TArgsStruct, represents the arguments of a Thrift method call, but as
 * pointers to the (const) parameter type to avoid copying.
 *
 * For the interface from the TArgsStruct example, TPargsStruct!Foo would be
 * equivalent to:
 * ---
 * struct FooBarPargs {
 *   const(string)* a;
 *   const(bool)* b;
 *
 *   void write(Protocol)(Protocol proto) const if (isTProtocol!Protocol);
 * }
 * ---
 */
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
      string memberId;
      string memberName;
      if (methodMetaFound && i < methodMeta.params.length) {
        memberId = to!string(methodMeta.params[i].id);
        memberName = methodMeta.params[i].name;
      } else {
        memberId = to!string(i + 1);
        memberName = "param" ~ to!string(i + 1);
      }

      // Workaround for DMD @@BUG@@ 6056: make an intermediary alias for the
      // parameter type, and declare the member using const(memberNameType)*.
      memberCode ~= "alias ParameterTypeTuple!(Interface." ~ methodName ~
        ")[" ~ to!string(i) ~ "] " ~ memberName ~ "Type;\n";
      memberCode ~= "const(" ~ memberName ~ "Type)* " ~ memberName ~ ";\n";

      fieldMetaCodes ~= "TFieldMeta(`" ~ memberName ~ "`, " ~ memberId ~ ")";
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
    code ~= "void write(P)(P proto) const if (isTProtocol!P) {\n";
    code ~= "writeStruct!(typeof(this), P, [" ~ ctfeJoin(fieldMetaCodes) ~
      "], true)(this, proto);\n";
    code ~= "}\n";
    code ~= "}\n";
    return code;
  }());
}

/**
 * A struct representing the result of a Thrift method call.
 *
 * It contains a field called "success" for the return value of the function
 * (with id 0), and additional fields for the exceptions declared for the
 * method, if any.
 *
 * Consider the following example:
 * ---
 * interface Foo {
 *   int bar(string a);
 *
 *   alias .FooException FooException;
 *
 *   enum methodMeta = [
 *     TMethodMeta("bar",
 *       [TParamMeta("a", 1)],
 *       [TExceptionMeta("fooe", 1, "FooException")]
 *     )
 *   ];
 * }
 * alias TResultStruct!Foo FooBarResult;
 * ---
 *
 * The definition of FooBarResult is equivalent to:
 * ---
 * struct FooBarResult {
 *   int success;
 *   FooException fooe;
 *
 *   mixin(TStructHelpers!([TFieldMeta("success", 0, TReq.OPTIONAL),
 *     TFieldMeta("fooe", 1, TReq.OPTIONAL)]));
 * }
 * ---
 *
 * If the TVerboseCodegen version is defined, a warning message is issued at
 * compilation if no TMethodMeta for Interface.methodName is found.
 */
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
        foreach (e; meta.front.exceptions) {
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

/**
 * Like TResultStruct, represents the result of a Thrift method call, but as
 * pointer to the return value to avoid copying.
 *
 * For the struct from the TResultStruct example, TPresultStruct!Foo would be
 * equivalent to:
 * ---
 * struct FooBarPresult {
 *   int* success;
 *   Foo.FooException fooe;
 *
 *   struct IsSetFlags {
 *     bool success;
 *   }
 *   IsSetFlags isSetFlags;
 *
 *   bool isSet(string fieldName)() const @property;
 *   void read(Protocol)(Protocol proto) if (isTProtocol!Protocol);
 * }
 * ---
 */
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
        foreach (e; meta.front.exceptions) {
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
      bool isSet(string fieldName)() const @property if (
        is(MemberType!(typeof(this), fieldName))
      ) {
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

    code ~= "void read(P)(P proto) if (isTProtocol!P) {\n";
    code ~= "readStruct!(typeof(this), P, [" ~ ctfeJoin(fieldMetaCodes) ~
      "], true)(this, proto);\n";
    code ~= "}\n";
    code ~= "}\n";
    return code;
  }());
}

/**
 * Thrift service client, which implements an interface by synchronously
 * calling a server over a TProtocol.
 *
 * TClientBase simply extends Interface with generic input/output protocol
 * properties to serve as a supertype for all TClients for the same service,
 * which might be instantiated with different concrete protocol types (there
 * is no covariance for template type parameters). If Interface is derived
 * from another interface BaseInterface, it also extends
 * TClientBase!BaseInterface.
 *
 * TClient is the class that actually implements TClientBase. Just as
 * TClientBase, it is also derived from TClient!BaseInterface for inheriting
 * services.
 *
 * TClient takes two optional template arguments which can be used for
 * specifying the actual TProtocol implementation used for optimization
 * purposes, as virtual calls can completely be eliminated then. If
 * OutputProtocol is not specified, it is assumed to be the same as
 * InputProtocol. The protocol properties defined by TClientBase are exposed
 * with their concrete type (return type covariance).
 *
 * In addition to implementing TClientBase!Interface, TClient offers the
 * following constructors:
 * ---
 * this(InputProtocol iprot, OutputProtocol oprot);
 * // Only if is(InputProtocol == OutputProtocol), to use the same protocol
 * // for both input and output:
 * this(InputProtocol prot);
 * ---
 *
 * The sequence id of the method calls starts at zero and is automatically
 * incremented.
 */
interface TClientBase(Interface) if (isBaseService!Interface) : Interface {
  /**
   * The input protocol used by the client.
   */
  TProtocol inputProtocol() @property;

  /**
   * The output protocol used by the client.
   */
  TProtocol outputProtocol() @property;
}

/// Ditto
interface TClientBase(Interface) if (isDerivedService!Interface) :
  TClientBase!(BaseService!Interface), Interface {}

/// Ditto
template TClient(Interface, InputProtocol = TProtocol, OutputProtocol = void) if (
  isService!Interface && isTProtocol!InputProtocol &&
  (isTProtocol!OutputProtocol || is(OutputProtocol == void))
) {
  mixin({
    static if (isDerivedService!Interface) {
      string code = "class TClient : TClient!(BaseService!Interface, " ~
        "InputProtocol, OutputProtocol), TClientBase!Interface {\n";
      code ~= q{
        this(IProt iprot, OProt oprot) {
          super(iprot, oprot);
        }

        static if (is(IProt == OProt)) {
          this(IProt prot) {
            super(prot);
          }
        }

        // DMD @@BUG@@: If these are not present in this class (would be)
        // inherited anyway, »not implemented« errors are raised.
        override IProt inputProtocol() @property {
          return super.inputProtocol;
        }
        override OProt outputProtocol() @property {
          return super.outputProtocol;
        }
      };
    } else {
      string code = "class TClient : TClientBase!Interface {";
      code ~= q{
        alias InputProtocol IProt;
        static if (isTProtocol!OutputProtocol) {
          alias OutputProtocol OProt;
        } else {
          static assert(is(OutputProtocol == void));
          alias InputProtocol OProt;
        }

        this(IProt iprot, OProt oprot) {
          iprot_ = iprot;
          oprot_ = oprot;
        }

        static if (is(IProt == OProt)) {
          this(IProt prot) {
            this(prot, prot);
          }
        }

        IProt inputProtocol() @property {
          return iprot_;
        }

        OProt outputProtocol() @property {
          return oprot_;
        }

        protected IProt iprot_;
        protected OProt oprot_;
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
          // Use the param name speficied in the meta information if any –
          // just cosmetics in this case.
          string paramName;
          if (methodMetaFound && i < methodMeta.params.length) {
            paramName = methodMeta.params[i].name;
          } else {
            paramName = "param" ~ to!string(i + 1);
          }

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
        code ~= "oprot_.writeMessageBegin(TMessage(`" ~ methodName ~
          "`, TMessageType.CALL, ++seqid_));\n";
        code ~= "args.write(oprot_);\n";
        code ~= "oprot_.writeMessageEnd();\n";
        code ~= "oprot_.transport.flush();\n";

        // If this is not a oneway method, generate the recieving code.
        if (!methodMetaFound || methodMeta.type != TMethodType.ONEWAY) {
          code ~= "TPresultStruct!(Interface, `" ~ methodName ~ "`) result;\n";

          if (!is(ReturnType!(mixin("Interface." ~ methodName)) == void)) {
            code ~= "ReturnType!(Interface." ~ methodName ~ ") _return;\n";
            code ~= "result.success = &_return;\n";
          }

          // TODO: The C++ implementation checks for matching method name here,
          // should we do as well?
          code ~= q{
            auto msg = iprot_.readMessageBegin();
            scope (exit) {
              iprot_.readMessageEnd();
              iprot_.transport.readEnd();
            }

            if (msg.type == TMessageType.EXCEPTION) {
              auto x = new TApplicationException(null);
              x.read(iprot_);
              iprot_.transport.readEnd();
              throw x;
            }
            if (msg.type != TMessageType.REPLY) {
              skip(iprot_, TType.STRUCT);
              iprot_.transport.readEnd();
            }
            if (msg.seqid != seqid_) {
              throw new TApplicationException(
                methodName ~ " failed: Out of sequence response.",
                TApplicationException.Type.BAD_SEQUENCE_ID
              );
            }
            result.read(iprot_);
          };

          if (methodMetaFound) {
            foreach (e; methodMeta.exceptions) {
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

/**
 * TClient construction helper to avoid having to explicitly specify
 * the protocol types, i.e. to allow the constructor being called using IFTI
 * (see $(LINK2 http://d.puremagic.com/issues/show_bug.cgi?id=6082, D Bugzilla
 * enhancement requet 6082)).
 */
TClient!(Interface, Prot) createTClient(Interface, Prot)(Prot prot) if (
  isService!Interface && isTProtocol!Prot
) {
  return new TClient!(Interface, Prot)(prot);
}

/// Ditto
TClient!(Interface, IProt, Oprot) createTClient(Interface, IProt, OProt)
  (IProt iprot, OProt oprot) if (
  isService!Interface && isTProtocol!IProt &&isTProtocol!OProt
) {
  return new TClient!(Interface, IProt, OProt)(iprot, oprot);
}

/**
 * Manages a pool of TClients for the given interface, forwarding RPC calls to
 * members of the pool.
 *
 * If a request fails, another client from the pool is tried, and optionally,
 * a client is disabled for a configurable amount of time if it fails too
 * often.
 *
 * If Interface extends another BaseInterface, TClientPool!Interface is also
 * derived from TClientPool!BaseInterface.
 */
// Note: The implementation is a bit peculiar in places to avoid copy/pasting
// the implementations between the derived and non-derived versions.
class TClientPool(Interface) if (isBaseService!Interface) : Interface {
  /// Shorthand for TClientBase!Interface, the client type this instance
  /// operates on.
  alias TClientBase!Interface Client;

  /**
   * Creates a new instance and adds the given clients to the pool.
   */
  this(Client[] clients) {
    clients_ = clients;

    isRpcException = (Exception e) {
      return (
        (cast(TTransportException)e !is null) ||
        (cast(TApplicationException)e !is null)
      );
    };
  }

  /**
   * Executes an operation on the first currently active client.
   *
   * If the operation fails (throws an exception for which isRpcException is
   * true), the failure is recorded and the next client in the pool is tried.
   *
   * Example:
   * ---
   * interface Foo { string bar(); }
   * auto poolClient = createTClientPool([new createTClient!Foo(someProtocol)]);
   * auto result = poolClient.execute((c){ return c.bar(); });
   * ---
   */
  ResultType execute(ResultType)(scope ResultType delegate(Client) work) {
    return executeOnPool(clients, work);
  }

  /**
   * Adds a client to the pool.
   */
  void addClient(Client client) {
    clients_ ~= client;
  }

  /**
   * Removes a client from the pool.
   *
   * Returns: Whether the client was found in the pool.
   */
  bool removeClient(Client client) {
    auto removed = remove!((a){ return a is client; })(clients_).length;
    clients_ = clients_[0 .. $ - removed];

    if (removed > 0) faultInfos_.remove(client);

    return (removed > 0);
  }

  mixin(poolForwardCode!Interface());

  /// Whether to use a random permutation of the client pool on every call to
  /// execute(). This can be used e.g. as a simple form of load balancing.
  ///
  /// Defaults to false.
  bool permuteClients = false;

  /// Whether to open the underlying transports of a client before trying to
  /// execute a method if they are not open. This is usually desirable
  /// because it allows e.g. to automatically reconnect to a remote server
  /// if the network connection is dropped.
  ///
  /// Defaults to true.
  bool reopenTransports = true;

  /// Called to determine whether an exception comes from a client from the
  /// pool not working properly, or if it an exception thrown at the
  /// application level.
  ///
  /// If the delegate returns true, the server/connection is considered to be
  /// at fault, if it returns false, the exception is just passed on to the
  /// caller.
  ///
  /// By default, returns true for instances of TTransportException and
  /// TApplicationException, false otherwise.
  bool delegate(Exception) isRpcException;

  /// Whether to keep trying to find a working client if all have failed in a
  /// row.
  ///
  /// Defaults to false.
  bool keepTrying = false;

  /// The number of consecutive faults after which a client is disabled until
  /// faultDisableDuration has passed. 0 to disable.
  ///
  /// Defaults to 0.
  ushort faultDisableCount = 0;

  /// The duration for which a client is no longer considered after it has
  /// failed too often.
  ///
  /// Defaults to one second.
  Duration faultDisableDuration = dur!"seconds"(1);

protected:
  // Actual implementation of execute(). Takes the list of clients as
  // parameter to avoid duplicating it in derived classes for the derived
  // client types.
  ResultType executeOnPool(ClientType, ResultType)(
    ClientType[] allClients,
    scope ResultType delegate(ClientType) work
  ) {
    if (allClients.empty) {
      throw new TException("No clients available to try.");
    }

    while (true) {
      ForwardRange!ClientType clients = inputRangeObject(allClients);

      if (permuteClients) {
        clients = inputRangeObject(randomCover(allClients, rndGen));
      }

      size_t clientsTried;

      while (!clients.empty) {
        auto client = clients.front;
        clients.popFront;
        auto faultInfo = client in faultInfos_;

        if (faultInfo && faultInfo.resetTime != faultInfo.resetTime.init) {
          // The argument to < needs to be an lvalue…
          auto currentTick = TickDuration.currSystemTick;
          if (faultInfo.resetTime < currentTick) {
            // The timeout expired, remove the client from the list and go
            // ahead trying it.
            faultInfos_.remove(client);
          } else {
            // The timeout didn't expire yet, try the next client.
            continue;
          }
        }

        ++clientsTried;

        try {
          scope (success) {
            if (faultInfo) faultInfos_.remove(client);
          }

          if (reopenTransports) {
            client.inputProtocol.transport.open();
            client.outputProtocol.transport.open();
          }

          return work(client);
        } catch (Exception e) {
          if (isRpcException && isRpcException(e)) {
            // If something went wrong on the transport layer, increment the
            // fault count.
            if (!faultInfo) {
              faultInfos_[client] = FaultInfo();
              faultInfo = client in faultInfos_;
            }

            ++faultInfo.count;
            if (faultInfo.count >= faultDisableCount) {
              // If the client has hit the fault count limit, disable it for
              // specified duration.
              faultInfo.resetTime = TickDuration.currSystemTick +
                cast(TickDuration)faultDisableDuration;
            }
          } else {
            // We are dealing with a normal exception thrown by the
            // server-side method, just pass it on. As far as we are
            // concerned, the method call succeded.
            if (faultInfo) faultInfos_.remove(client);
            throw e;
          }
        }
      }

      // If we get here, no client succeeded during the current iteration.
      if (!keepTrying) {
        // TODO: Would it make sense to collect all client exceptions and pass
        // them out here?
        throw new TException("All clients failed.");
      }

      if (clientsTried == 0) {
        // All clients are currently disabled, sleep until a timeout expires
        // to avoid spinning.
        auto first = reduce!"min(a, b)"(map!"a.resetTime"(faultInfos_.values));
        auto toSleep = to!Duration(first - TickDuration.currSystemTick);
        if (toSleep > dur!"hnsecs"(0)) {
          import core.thread;
          Thread.sleep(toSleep);
        }
      }
    }
  }

  // Returns the list of clients in the pool. This is a kludge to enable the
  // same string mixins to be used for both base and derived classes via
  // return type covariance.
  @property Client[] clients() {
    return clients_;
  }

private:
  Client[] clients_;

  static struct FaultInfo {
    ushort count;
    TickDuration resetTime;
  }
  FaultInfo[Client] faultInfos_;
}

/// Ditto
class TClientPool(Interface) if (isDerivedService!Interface) :
  TClientPool!(BaseService!Interface), Interface
{
  alias TClientBase!Interface Client;

  this(Client[] clients) {
    super(clients);
  }

  ResultType execute(ResultType)(scope ResultType delegate(Client) work) {
    return execute(clients, work);
  }

  mixin(poolForwardCode!Interface());
protected:
  override @property Client[] clients() {
    // We know that the clients of the super class are actually of the
    // derived type, since we put them there in our constructor.
    return cast(Client[])super.clients;
  }
}

private {
  // Cannot use an anonymous delegate literal for this because they aren't
  // allowed in class scope.
  static string poolForwardCode(Interface)() {
    string code = "";

    foreach (methodName; __traits(derivedMembers, Interface)) {
      enum qn = "Interface." ~ methodName;
      static if (isSomeFunction!(mixin(qn))) {
        code ~= "ReturnType!(" ~ qn ~ ") " ~ methodName ~
          "(ParameterTypeTuple!(" ~ qn ~ ") args) {\n";
        code ~= "return executeOnPool(clients, " ~
          "(Client c){ return c." ~ methodName ~ "(args); });\n";
        code ~= "}\n";
      }
    }

    return code;
  }
}

/**
 * TClientPool construction helper to avoid having to explicitly specify
 * the interface type, i.e. to allow the constructor being called using IFTI
 * (see $(LINK2 http://d.puremagic.com/issues/show_bug.cgi?id=6082, D Bugzilla
 * enhancement requet 6082)).
 */
TClientPool!Interface createTClientPool(Interface)(
  TClientBase!Interface[] clients
) if (isService!Interface) {
  return new typeof(return)(clients);
}

/**
 * Asynchronous Thrift service client, which just like TClient implements an
 * interface by calling a server, but instead of synchronously invoking the
 * methods, it returns the results as TFuture and uses a TAsyncManager to
 * perform the actual work.
 *
 * The generated class »almost« implements the given interface, with the
 * exception of returning TFutures instead of the plain return values, and
 * offers two constructors with the following signatures:
 * ---
 * this(TAsyncTransport trans, TTransportFactory tf, TProtocolFactory pf);
 * this(TAsyncTransport trans, TTransportFactory itf, TTransportFactory otf,
 *   TProtocolFactory ipf, TProtocolFactory opf);
 * ---
 *
 * As you can see, TAsyncClient requires a TAsyncTransport to be passed, which
 * is understandable, because it needs a way to access the associated
 * TAsyncManager. To set up any wrapper transports (e.g. buffered, framed) on
 * top of it and to instanciate the protocols to use, the constructors accept
 * TTransportFactory and TProtocolFactory instances – the three argument
 * constructor is a shortcut if the same transport and protocol are to be used
 * for both input and output, which is probably the most common case.
 *
 * Just as TClient does, TAsyncClient also takes two optional template
 * arguments which can be used for specifying the actual TProtocol
 * implementation used for optimization purposes, as virtual calls can
 * completely be eliminated then. If the actual types of the protocols
 * instantiated by the factories used does not match the ones statically
 * specified in the template parameters, a TException is thrown during
 * construction.
 */
template TAsyncClient(Interface, InputProtocol = TProtocol, OutputProtocol = void) if (
  isService!Interface && isTProtocol!InputProtocol &&
  (isTProtocol!OutputProtocol || is(OutputProtocol == void))
) {
  mixin({
    static if (is(Interface BaseInterfaces == super) && BaseInterfaces.length > 0) {
      static assert(BaseInterfaces.length == 1,
        "Services cannot be derived from more than one parent.");

      string code = "class TAsyncClient : TAsyncClient!(" ~
        "BaseService!Interface, InputProtocol, OutputProtocol) {\n";
      code ~= q{
        this(TAsyncTransport trans, TTransportFactory tf, TProtocolFactory pf) {
          this(trans, tf, tf, pf, pf);
        }

        this(TAsyncTransport trans, TTransportFactory itf,
          TTransportFactory otf, TProtocolFactory ipf, TProtocolFactory opf
        ) {
          super(trans, itf, otf, ipf, opf);
          client_ = new typeof(client_)(iprot_, oprot_);
        }

        private TClient!(Interface, IProt, OProt) client_;
      };
    } else {
      string code = "class TAsyncClient {";
      code ~= q{
        alias InputProtocol IProt;
        static if (isTProtocol!OutputProtocol) {
          alias OutputProtocol OProt;
        } else {
          static assert(is(OutputProtocol == void));
          alias InputProtocol OProt;
        }

        this(TAsyncTransport trans, TTransportFactory tf, TProtocolFactory pf) {
          this(trans, tf, tf, pf, pf);
        }

        this(TAsyncTransport trans, TTransportFactory itf,
          TTransportFactory otf, TProtocolFactory ipf, TProtocolFactory opf
        ) {
          asyncTransport_ = trans;

          auto iprot = ipf.getProtocol(itf.getTransport(trans));
          iprot_ = cast(IProt)iprot;
          enforce(iprot_, new TException(text("Input protocol not of the " ~
            "specified concrete type (", IProt.stringof, ").")));

          auto oprot = opf.getProtocol(otf.getTransport(trans));
          oprot_ = cast(OProt)oprot;
          enforce(oprot_, new TException(text("Output protocol not of the " ~
            "specified concrete type (", OProt.stringof, ").")));

          client_ = new typeof(client_)(iprot_, oprot_);
        }

        protected TAsyncTransport asyncTransport_;
        protected IProt iprot_;
        protected OProt oprot_;
        private TClient!(Interface, IProt, OProt) client_;
      };
    }

    foreach (methodName; __traits(derivedMembers, Interface)) {
      static if (isSomeFunction!(mixin("Interface." ~ methodName))) {
        string[] paramList;
        string[] paramNames;
        foreach (i, _; ParameterTypeTuple!(mixin("Interface." ~ methodName))) {
          immutable paramName = "param" ~ to!string(i + 1);
          paramList ~= "ParameterTypeTuple!(Interface." ~ methodName ~ ")[" ~
            to!string(i) ~ "] " ~ paramName;
          paramNames ~= paramName;
        }

        immutable returnTypeCode = "ReturnType!(Interface." ~ methodName ~ ")";

        code ~= "TFuture!(" ~ returnTypeCode ~ ") " ~ methodName ~ "(" ~
          ctfeJoin(paramList) ~ ") {\n";

        // Create the future instance that will repesent the result.
        code ~= "auto promise = new TPromise!(" ~ returnTypeCode ~ ");\n";

        // Prepare work item that executes the TClient method call.
        code ~= "auto work = TAsyncWorkItem(asyncTransport_, {\n";
        code ~= "try {\n";
        code ~= "static if (is(ReturnType!(Interface." ~ methodName ~
          ") == void)) {\n";
        code ~= "client_." ~ methodName ~ "(" ~ ctfeJoin(paramNames) ~ ");\n";
        code ~= "promise.succeed();\n";
        code ~= "} else {\n";
        code ~= "auto result = client_." ~ methodName ~ "(" ~
          ctfeJoin(paramNames) ~ ");\n";
        code ~= "promise.succeed(result);\n";
        code ~= "}\n";
        code ~= "} catch (Exception e) {\n";
        code ~= "promise.fail(e);\n";
        code ~= "}\n";
        code ~= "});\n";

        // Enqueue the work item and immediately return the promise resp. its
        // future interface.
        code ~= "asyncTransport_.asyncManager.execute(work);\n";
        code ~= "return promise;\n";
        code ~= "}\n";
      }
    }

    code ~= "}\n";
    return code;
  }());
}

/**
 * Service processor for Interface, which implements TProcessor by
 * synchronously forwarding requests for the service methods to a handler
 * implementing Interface.
 *
 * The generated class implements TProcessor and additionally allows a
 * TProcessorEventHandler to be specified via public eventHandler property.
 * The constructor takes a single argument of type Interface, which is the
 * handler to forward the requests to. If Interface is derived from another
 * interface BaseInterface, this class is also derived from
 * TServiceProcessor!BaseInterface.
 *
 * The optional Protocols template tuple parameter can be used to specify
 * one or more TProtocol implementations to specifically generate code for. If
 * the actual types of the protocols passed to process() at runtime match one
 * of the items from the list, the optimized code paths are taken, otherwise,
 * a generic TProtocol version is used as fallback. For cases where the input
 * and output protocols differ, ProtocolPair!(InputProtocol, OutputProtocol)
 * can be used in the Protocols list.
 */
template TServiceProcessor(Interface, Protocols...) if (
  isService!Interface && allSatisfy!(isTProtocolOrPair, Protocols)
) {
  mixin({
    static if (is(Interface BaseInterfaces == super) && BaseInterfaces.length > 0) {
      static assert(BaseInterfaces.length == 1,
        "Services cannot be derived from more than one parent.");

      string code = "class TServiceProcessor : " ~
        "TServiceProcessor!(BaseService!Interface) {\n";
      code ~= "private Interface iface_;\n";

      string constructorCode = "this(Interface iface) {\n";
      constructorCode ~= "super(iface);\n";
      constructorCode ~= "iface_ = iface;\n";
    } else {
      string code = "class TServiceProcessor : TProcessor {";
      code ~= q{
        override bool process(TProtocol iprot, TProtocol oprot, Variant context) {
          auto msg = iprot.readMessageBegin();

          void writeException(TApplicationException e) {
            oprot.writeMessageBegin(TMessage(msg.name, TMessageType.EXCEPTION,
              msg.seqid));
            e.write(oprot);
            oprot.writeMessageEnd();
            oprot.transport.writeEnd();
            oprot.transport.flush();
          }

          if (msg.type != TMessageType.CALL && msg.type != TMessageType.ONEWAY) {
            skip(iprot, TType.STRUCT);
            iprot.readMessageEnd();
            iprot.transport.readEnd();

            writeException(new TApplicationException(
              TApplicationException.Type.INVALID_MESSAGE_TYPE));
            return false;
          }

          auto dg = msg.name in processMap_;
          if (!dg) {
            skip(iprot, TType.STRUCT);
            iprot.readMessageEnd();
            iprot.transport.readEnd();

            writeException(new TApplicationException("Invalid method name: '" ~
              msg.name ~ "'.", TApplicationException.Type.INVALID_MESSAGE_TYPE));

            return false;
          }

          (*dg)(msg.seqid, iprot, oprot, context);
          return true;
        }

        TProcessorEventHandler eventHandler;

        alias void delegate(int, TProtocol, TProtocol, Variant) ProcessFunc;
        protected ProcessFunc[string] processMap_;
        private Interface iface_;
      };

      string constructorCode = "this(Interface iface) {\n";
      constructorCode ~= "iface_ = iface;\n";
    }

    // Generate the handling code for each method, consisting of the dispatch
    // function, registering it in the constructor, and the actual templated
    // handler function.
    foreach (methodName; __traits(derivedMembers, Interface)) {
      static if (isSomeFunction!(mixin("Interface." ~ methodName))) {
        // Register the processing function in the constructor.
        immutable procFuncName = "process_" ~ methodName;
        immutable dispatchFuncName = procFuncName ~ "_protocolDispatch";
        constructorCode ~= "processMap_[`" ~ methodName ~ "`] = &" ~
          dispatchFuncName ~ ";\n";

        bool methodMetaFound;
        TMethodMeta methodMeta;
        static if (is(typeof(Interface.methodMeta) : TMethodMeta[])) {
          enum meta = find!`a.name == b`(Interface.methodMeta, methodName);
          if (!meta.empty) {
            methodMetaFound = true;
            methodMeta = meta.front;
          }
        }

        // The dispatch function to call the specialized handler functions. We
        // test the protocols if they can be converted to one of the passed
        // protocol types, and if not, fall back to the generic TProtocol
        // version of the processing function.
        code ~= "void " ~ dispatchFuncName ~
          "(int seqid, TProtocol iprot, TProtocol oprot, Variant context) {\n";
        code ~= "foreach (Protocol; TypeTuple!(Protocols, TProtocol)) {\n";
        code ~= q{
          static if (is(Protocol _ : ProtocolPair!(I, O), I, O)) {
            alias I IProt;
            alias O OProt;
          } else {
            alias Protocol IProt;
            alias Protocol OProt;
          }
          auto castedIProt = cast(IProt)iprot;
          auto castedOProt = cast(OProt)oprot;
        };
        code ~= "if (castedIProt && castedOProt) {\n";
        code ~= procFuncName ~
          "!(IProt, OProt)(seqid, castedIProt, castedOProt, context);\n";
        code ~= "return;\n";
        code ~= "}\n";
        code ~= "}\n";
        code ~= "throw new TException(`Internal error: Null iprot/oprot " ~
          "passed to processor protocol dispatch function.`);\n";
        code ~= "}\n";

        // The actual handler function, templated on the input and output
        // protocol types.
        code ~= "void " ~ procFuncName ~ "(IProt, OProt)(int seqid, IProt " ~
          "iprot, OProt oprot, Variant connectionContext) " ~
          "if (isTProtocol!IProt && isTProtocol!OProt) {\n";
        code ~= "TArgsStruct!(Interface, `" ~ methodName ~ "`) args;\n";

        // Store the (qualified) method name in a manifest constant to avoid
        // having to litter the code below with lots of string manipulation.
        code ~= "enum methodName = `" ~ methodName ~ "`;\n";

        code ~= q{
          enum qName = Interface.stringof ~ "." ~ methodName;

          Variant callContext;
          if (eventHandler) {
            callContext = eventHandler.createContext(qName, connectionContext);
          }

          scope (exit) {
            if (eventHandler) {
              eventHandler.deleteContext(callContext, qName);
            }
          }

          if (eventHandler) eventHandler.preRead(callContext, qName);

          args.read(iprot);
          iprot.readMessageEnd();
          iprot.transport.readEnd();

          if (eventHandler) eventHandler.postRead(callContext, qName);
        };

        code ~= "TResultStruct!(Interface, `" ~ methodName ~ "`) result;\n";
        code ~= "try {\n";

        // Generate the parameter list to pass to the called iface function.
        string[] paramList;
        foreach (i, _; ParameterTypeTuple!(mixin("Interface." ~ methodName))) {
          string paramName;
          if (methodMetaFound && i < methodMeta.params.length) {
            paramName = methodMeta.params[i].name;
          } else {
            paramName = "param" ~ to!string(i + 1);
          }
          paramList ~= "args." ~ paramName;
        }

        immutable call = "iface_." ~ methodName ~ "(" ~ ctfeJoin(paramList) ~ ")";
        if (is(ReturnType!(mixin("Interface." ~ methodName)) == void)) {
          code ~= call ~ ";\n";
        } else {
          code ~= "result.set!`success`(" ~ call ~ ");\n";
        }

        // If this is not a oneway method, generate the recieving code.
        if (!methodMetaFound || methodMeta.type != TMethodType.ONEWAY) {
          if (methodMetaFound) {
            foreach (e; methodMeta.exceptions) {
              code ~= "} catch (Interface." ~ e.type ~ " " ~ e.name ~ ") {\n";
              code ~= "result.set!`" ~ e.name ~ "`(" ~ e.name ~ ");\n";
            }
          }
          code ~= "}\n";

          code ~= q{
            catch (Exception e) {
              if (eventHandler) {
                eventHandler.handlerError(callContext, qName, e);
              }

              auto x = new TApplicationException(to!string(e));
              oprot.writeMessageBegin(
                TMessage(methodName, TMessageType.EXCEPTION, seqid));
              x.write(oprot);
              oprot.writeMessageEnd();
              oprot.transport.writeEnd();
              oprot.transport.flush();
              return;
            }

            if (eventHandler) eventHandler.preWrite(callContext, qName);

            oprot.writeMessageBegin(TMessage(methodName,
              TMessageType.REPLY, seqid));
            result.write(oprot);
            oprot.writeMessageEnd();
            oprot.transport.writeEnd();
            oprot.transport.flush();

            if (eventHandler) eventHandler.postWrite(callContext, qName);
          };
        } else {
          // For oneway methods, we obviously cannot notify the client of any
          // exceptions, just call the event handler if one is set.
          code ~= "}\n";
          code ~= q{
            catch (Exception e) {
              if (eventHandler) {
                eventHandler.handlerError(callContext, qName, e);
              }
              return;
            }

            if (eventHandler) eventHandler.onewayComplete(callContext, qName);
          };
        }
        code ~= "}\n";
      }
    }

    code ~= constructorCode ~ "}\n";
    code ~= "}\n";

    return code;
  }());
}

/// Ditto
struct ProtocolPair(InputProtocol, OutputProtocol) if (
  isTProtocol!InputProtocol && isTProtocol!OutputProtocol
) {}

/**
 * true if T represents a Thrift service.
 */
template isService(T) {
  enum isService = isBaseService!T || isDerivedService!T;
}

/**
 * true if T represents a Thrift service not derived from another service.
 */
template isBaseService(T) {
  static if(is(T _ == interface) &&
    (!is(T TBases == super) || TBases.length == 0)
  ) {
    enum isBaseService = true;
  } else {
    enum isBaseService = false;
  }
}

/**
 * true if T represents a Thrift service derived from another service.
 */
template isDerivedService(T) {
  static if(is(T _ == interface) &&
    is(T TBases == super) && TBases.length == 1
  ) {
    enum isDerivedService = true;
  } else {
    enum isDerivedService = false;
  }
}

/**
 * For derived services, gets the base service interface.
 */
template BaseService(T) if (isDerivedService!T) {
  alias BaseTypeTuple!T[0] BaseService;
}

/*
 * Removes all type qualifiers from T.
 *
 * In contrast to std.traits.Unqual, FullyUnqual also removes qualifiers from
 * array elements (e.g. immutable(byte[]) -> byte[], not immutable(byte)[]),
 * excluding strings (string isn't reduced to char[]).
 *
 * Must be public because it is used by generated code, but is not part of the
 * public API.
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
  template isTProtocol(T) {
    enum isTProtocol = is(T : TProtocol);
  }

  unittest {
    static assert(isTProtocol!TProtocol);
    static assert(!isTProtocol!void);
  }

  template isProtocolPair(T) {
    static if (is(T _ == ProtocolPair!(I, O), I, O)) {
      enum isProtocolPair = true;
    } else {
      enum isProtocolPair = false;
    }
  }

  unittest {
    static assert(isProtocolPair!(ProtocolPair!(TProtocol, TProtocol)));
    static assert(!isProtocolPair!TProtocol);
  }

  template isTProtocolOrPair(T) {
    enum isTProtocolOrPair = isTProtocol!T || isProtocolPair!T;
  }

  unittest {
    static assert(isTProtocolOrPair!TProtocol);
    static assert(isTProtocolOrPair!(ProtocolPair!(TProtocol, TProtocol)));
    static assert(!isTProtocolOrPair!void);
  }

  /*
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

  /*
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
