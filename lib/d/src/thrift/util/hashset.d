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
module thrift.util.hashset;

import std.algorithm : join, map;
import std.traits : isImplicitlyConvertible, ParameterTypeTuple;
import std.range : ElementType, isInputRange;

/**
 * A quickly hacked together hash set implementation backed by built-in
 * associative arrays to have something to compile Thrift's set<> to until
 * std.container gains something suitable.
 */
// Note: The funky pointer casts (i.e. *(cast(immutable(E)*)&e) instead of
// just cast(immutable(E))e) are a workaround for LDC 2 compatibilty.
final class HashSet(E) {
  ///
  this() {}

  ///
  this(E[] elems...) {
    insert(elems);
  }

  ///
  void insert(Stuff)(Stuff stuff) if (isImplicitlyConvertible!(Stuff, E)) {
    aa_[*(cast(immutable(E)*)&stuff)] = [];
  }

  ///
  void insert(Stuff)(Stuff stuff) if (
    isInputRange!Stuff && isImplicitlyConvertible!(ElementType!Stuff, E)
  ) {
    foreach (e; stuff) {
      aa_[*(cast(immutable(E)*)&e)] = [];
    }
  }

  ///
  void opOpAssign(string op : "~", Stuff)(Stuff stuff) {
    insert(stuff);
  }

  ///
  void remove(E e) {
    aa_.remove(*(cast(immutable(E)*)&e));
  }
  alias remove removeKey;

  ///
  void removeAll() {
    aa_ = null;
  }

  ///
  size_t length() @property const {
    return aa_.length;
  }

  ///
  size_t empty() @property const {
    return !aa_.length;
  }

  ///
  E* opBinaryRight(string op : "in")(E e) const {
    return e in aa_;
  }

  ///
  E[] opSlice() const {
    return cast(E[])(aa_.keys);
  }

  ///
  int opApply(scope int delegate(ref E elem) dg) const {
    return aa_.byKey()(cast(ParameterTypeTuple!(typeof(aa_.byKey()))[0]) dg);
  }

  ///
  override string toString() const {
    // Only provide toString() if to!string() is available for E (exceptions are
    // e.g. delegates).
    import std.conv;
    static if (is(typeof(to!string(E.init)) : string)) {
      return "{" ~ join(map!`to!string(a)`(aa_.keys), ", ") ~ "}";
    } else {
      return (cast()super).toString();
    }
  }

  ///
  override bool opEquals(Object other) const {
    auto rhs = cast(const(HashSet))other;
    if (rhs) {
      return aa_ == rhs.aa_;
    }

    return super.opEquals(other);
  }

private:
  alias void[0] Void;
  Void[immutable(E)] aa_;
}

/// Ditto
auto hashSet(E)(E[] elems...) {
  return new HashSet!E(elems);
}

unittest {
  auto a = hashSet(1, 2, 2, 3);
  assert(a.length == 3);
  assert(2 in a);
  assert(5 !in a);
  assert(a.toString().length == 9);
  a.remove(2);
  assert(a.length == 2);
  assert(2 !in a);
  a.removeAll();
  assert(a.empty);
  assert(a.toString == "{}");

  void delegate() dg;
  auto b = hashSet(dg);
  assert(b.toString() == "thrift.util.hashset.HashSet!(void delegate()).HashSet");
}
