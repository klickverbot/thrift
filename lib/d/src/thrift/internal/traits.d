module thrift.internal.traits;

import std.traits : isSomeString;
import thrift.base;

/*
 * Removes all type qualifiers from T.
 *
 * In contrast to std.traits.Unqual, FullyUnqual also removes qualifiers from
 * array elements (e.g. immutable(byte[]) -> byte[], not immutable(byte)[]),
 * excluding strings (string isn't reduced to char[]).
 *
 * Must be public because it is used by generated code.
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
