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

module thrift.internal.codegen;

import std.traits : InterfacesTuple, isSomeString;
import std.typetuple : staticMap, NoDuplicates, TypeTuple;
import thrift.codegen.base;

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

/**
 * true if null can be assigned to the passed type, false if not.
 */
template isNullable(T) {
  enum isNullable = __traits(compiles, { T t = null; });
}

template isStruct(T) {
  enum isStruct = is(T == struct);
}

template isException(T) {
 enum isException = is(T : Exception);
}

template isEnum(T) {
  enum isEnum = is(T == enum);
}

/**
 * Aliases itself to T.name.
 */
template GetMember(T, string name) {
  mixin("alias T." ~ name ~ " GetMember;");
}

/**
 * Aliases itself to typeof(symbol).
 */
template TypeOf(alias symbol) {
  alias typeof(symbol) TypeOf;
}

/**
 * Aliases itself to the type of the T member called name.
 */
alias Compose!(TypeOf, GetMember) MemberType;

/**
 * Returns the field metadata array for T if any, or an empty array otherwise.
 */
template getFieldMeta(T) if (isStruct!T || isException!T) {
  static if (is(typeof(T.fieldMeta) == TFieldMeta[])) {
    enum getFieldMeta = T.fieldMeta;
  } else {
    enum TFieldMeta[] getFieldMeta = [];
  }
}

/**
 * Merges the field metadata array for D with the passed array.
 */
template mergeFieldMeta(T, alias fieldMetaData = cast(TFieldMeta[])null) {
  // Note: We don't use getFieldMeta here to avoid bug if it is instantiated
  // from TIsSetFlags, see comment there.
  static if (is(typeof(T.fieldMeta) == TFieldMeta[])) {
    enum mergeFieldMeta = T.fieldMeta ~ fieldMetaData;
  } else {
    enum TFieldMeta[] mergeFieldMeta = fieldMetaData;
  }
}

/**
 * Returns the field requirement level for T.name.
 */
template memberReq(T, string name, alias fieldMetaData = cast(TFieldMeta[])null) {
  enum memberReq = memberReqImpl!(T, name, fieldMetaData).result;
}

private {
  template memberReqImpl(T, string name, alias fieldMetaData) {
    enum meta = find!`a.name == b`(mergeFieldMeta!(T, fieldMetaData), name);
    static if (meta.empty || meta.front.req == TReq.AUTO) {
      static if (isNullable!(MemberType!(T, name))) {
        enum result = TReq.OPTIONAL;
      } else {
        enum result = TReq.REQUIRED;
      }
    } else {
      enum result = meta.front.req;
    }
  }
}

/**
 * Returns the method metadata array for T if any, or an empty array otherwise.
 */
template getMethodMeta(T) if (isService!T) {
  static if (is(typeof(T.methodMeta) == TMethodMeta[])) {
    enum getMethodMeta = T.methodMeta;
  } else {
    enum TMethodMeta[] getMethodMeta = [];
  }
}


/**
 * true if T.name is a member variable. Exceptions include methods, static
 * members, artifacts like package aliases, …
 */
template isValueMember(T, string name) {
  static if (!is(MemberType!(T, name))) {
    enum isValueMember = false;
  } else static if (
    is(MemberType!(T, name) == void) ||
    isSomeFunction!(MemberType!(T, name)) ||
    __traits(compiles, { return mixin("T." ~ name); }())
  ) {
    enum isValueMember = false;
  } else {
    enum isValueMember = true;
  }
}

/**
 * Returns a tuple containing the member variables of T, not including
 * inherited fields.
 */
template valueMemberNames(T) {
  alias StaticFilter!(PApply!(isValueMember, T), __traits(derivedMembers, T))
    valueMemberNames;
}

template derivedMembers(T) {
  alias TypeTuple!(__traits(derivedMembers, T)) derivedMembers;
}

template AllMemberMethodNames(T) if (isService!T) {
  alias NoDuplicates!(
    FilterMethodNames!(
      T,
      staticMap!(
        derivedMembers,
        TypeTuple!(T, InterfacesTuple!T)
      )
    )
  ) AllMemberMethodNames;
}

private template FilterMethodNames(T, MemberNames...) {
  alias StaticFilter!(
    CompilesAndTrue!(
      Compose!(isSomeFunction, TypeOf, PApply!(GetMember, T))
    ),
    MemberNames
  ) FilterMethodNames;
}

/**
 * Returns a type tuple containing only the elements of T for which the
 * eponymous template predicate pred is true.
 *
 * Example:
 * ---
 * alias StaticFilter!(isIntegral, int, string, long, float[]) Filtered;
 * static assert(is(Filtered == TypeTuple!(int, long)));
 * ---
 */
template StaticFilter(alias pred, T...) {
  static if (T.length == 0) {
    alias TypeTuple!() StaticFilter;
  } else static if (pred!(T[0])) {
    alias TypeTuple!(T[0], StaticFilter!(pred, T[1 .. $])) StaticFilter;
  } else {
    alias StaticFilter!(pred, T[1 .. $]) StaticFilter;
  }
}

/**
 * Binds the first n arguments of a template to a particular value (where n is
 * the number of arguments passed to PApply).
 *
 * Example:
 * ---
 * struct Foo(T, U, V) {}
 * alias PApply!(Foo, int, long) PartialFoo;
 * static assert(is(PartialFoo!float == Foo!(int, long, float)));
 * ---
 */
template PApply(alias Target, T...) {
  template PApply(U...) {
    alias Target!(T, U) PApply;
  }
}
unittest {
  struct Test(T, U, V) {}
  alias PApply!(Test, int, long) PartialTest;
  static assert(is(PartialTest!float == Test!(int, long, float)));
}

/**
 * Composes a number of templates. The result is a template equivalent to
 * all the passed templates evaluated from right to left, akin to the
 * mathematical function composition notation: Instantiating Compose!(A, B, C)
 * is the same as instantiating A!(B!(C!(…))).
 *
 * This is especially useful for creating a template to use with staticMap/
 * StaticFilter, as demonstrated below.
 *
 * Example:
 * ---
 * template AllMethodNames(T) {
 *   alias StaticFilter!(
 *     CompilesAndTrue!(
 *       Compose!(isSomeFunction, TypeOf, PApply!(GetMember, T))
 *     ),
 *     __traits(allMembers, T)
 *   ) AllMethodNames;
 * }
 *
 * pragma(msg, AllMethodNames!Object);
 * ---
 */
template Compose(T...) {
  static if (T.length == 0) {
    template Compose(U...) {
      alias U Compose;
    }
  } else {
    template Compose(U...) {
      alias Instantiate!(T[0], Instantiate!(.Compose!(T[1 .. $]), U)) Compose;
    }
  }
}

/**
 * Instantiates the given template with the given list of parameters.
 *
 * Used to work around syntactic limiations of D with regard to instantiating
 * a template from a type tuple (e.g. T[0]!(...) is not valid) or a template
 * returning another template (e.g. Foo!(Bar)!(Baz) is not allowed).
 */
template Instantiate(alias Template, Params...) {
  alias Template!Params Instantiate;
}

/**
 * Combines several template predicates using logical AND, i.e. instantiating
 * All!(a, b, c) with parameters P for some templates a, b, c is equivalent to
 * a!P && b!P && c!P.
 *
 * The templates are evaluated from left to right, aborting evaluation in a
 * shurt-cut manner if a false result is encountered, in which case the latter
 * instantiations do not need to compile.
 */
template All(T...) {
  static if (T.length == 0) {
    template All(U...) {
      enum All = true;
    }
  } else {
    template All(U...) {
      static if (Instantiate!(T[0], U)) {
        alias Instantiate!(.All!(T[1 .. $]), U) All;
      } else {
        enum All = false;
      }
    }
  }
}

/**
 * Combines several template predicates using logical OR, i.e. instantiating
 * Any!(a, b, c) with parameters P for some templates a, b, c is equivalent to
 * a!P || b!P || c!P.
 *
 * The templates are evaluated from left to right, aborting evaluation in a
 * shurt-cut manner if a true result is encountered, in which case the latter
 * instantiations do not need to compile.
 */
template Any(T...) {
  static if (T.length == 0) {
    template Any(U...) {
      enum Any = false;
    }
  } else {
    template Any(U...) {
      static if (Instantiate!(T[0], U)) {
        enum Any = true;
      } else {
        alias Instantiate!(.Any!(T[1 .. $]), U) Any;
      }
    }
  }
}

/**
 * Negates the passed template predicate.
 */
template not(T...) if (T.length == 1 && is(typeof(!T[0]) : bool)) {
  enum bool not = !val;
}
alias PApply!(Compose, not) Not;

unittest {
  alias Not!hasType hasNoType;
  pragma(msg, hasNoType!true, hasNoType!int);
}

template ConfinedTuple(T...) {
  alias T Tuple;
  enum length = T.length;
}

/*
 * foreach (Item; Items) {
 *   List = Operator!(Item, List);
 * }
 * where Items is a ConfinedTuple and List is a type tuple.
 */
template ForAllWithList(alias Items, alias Operator, List...) if (
  is(typeof(Items.length) : size_t)
){
  static if (Items.length == 0) {
    alias List ForAllWithList;
  } else {
    alias .ForAllWithList!(
      ConfinedTuple!(Items.Tuple[1 .. $]),
      Operator,
      Operator!(Items.Tuple[0], List)
    ) ForAllWithList;
  }
}

/**
 * Wraps the passed template predicate so it returns true if it compiles and
 * evaluates to true, false it it doesn't compile or evaluates to false.
 */
template CompilesAndTrue(alias T) {
  template CompilesAndTrue(U...) {
    static if (is(typeof(T!U) : bool)) {
      enum bool CompilesAndTrue = T!U;
    } else {
      enum bool CompilesAndTrue = false;
    }
  }
}
