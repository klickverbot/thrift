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

import std.traits : isSomeString;
import std.typetuple : TypeTuple;
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

/**
 * Returns the type of the T member called name.
 */
template MemberType(T, string name) {
  alias typeof(__traits(getMember, T.init, name)) MemberType;
}

/**
 * Returns the field metadata array for T if any, or an empty array otherwise.
 */
template getFieldMeta(T) {
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
  enum mergeFieldMeta = getFieldMeta!T ~ fieldMetaData;
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
    static if (meta.empty) {
      enum result = TReq.OPT_IN_REQ_OUT;
    } else {
      enum result = meta.front.req;
    }
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
    is(typeof({ return mixin("T." ~ name); }()))
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
  alias staticFilter!(PApply!(isValueMember, T), __traits(derivedMembers, T))
    valueMemberNames;
}

template getMember(T, string name) {
  mixin("alias T." ~ name ~ " getMember;");
}

template hasType(alias T) {
  enum hasType = is(typeof(T));
}

/**
 * Returns a tuple containing only the elements of T for which pred compiles
 * and is true.
 */
template staticFilter(alias pred, T...) {
  static if (T.length == 0) {
    alias TypeTuple!() staticFilter;
  } else static if (is(typeof(pred!(T[0])) : bool)) {
    static if (pred!(T[0])) {
      alias TypeTuple!(T[0], staticFilter!(pred, T[1 .. $])) staticFilter;
    } else {
      alias staticFilter!(pred, T[1 .. $]) staticFilter;
    }
  } else {
    alias staticFilter!(pred, T[1 .. $]) staticFilter;
  }
}

/**
 * Binds the first n arguments of a template to a particular value (where n is
 * the number of arguments passed to PApply).
 *
 * Example:
 * ---
 * struct Foo(T, U, V) {}
 * alias PApply!(Foo, A, B) PartialFoo;
 * assert(is(PartialFoo!(C) == Foo!(A, B, C)));
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

template Instantiate(alias Template, Params...) {
  alias Template!Params Instantiate;
}

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

template Not(alias T) {
  template Not(U...) {
    enum Not = !T!U;
  }
}

template ConfinedTuple(T...) {
  alias T Tuple;
}

/*
 * foreach (Item; Items) {
 *   List = Operator!(Item, List);
 * }
 * where Items is a ConfinedTuple.
 */
template ForAllWithList(alias Items, alias Operator, List...) if (
  is(typeof(Items.Tuple.length) : size_t)
){
  static if (Items.Tuple.length == 0) {
    alias List ForAllWithList;
  } else {
    alias ForAllWithList!(
      ConfinedTuple!(Items.Tuple[1 .. $]),
      Operator,
      Operator!(Items.Tuple[0], List)
    ) ForAllWithList;
  }
}
