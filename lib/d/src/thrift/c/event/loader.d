module thrift.c.event.loader;

import std.traits : isFunctionPointer;
import thrift.c.loader;

void bindFunctions(alias Module)() {
  foreach (m; __traits(derivedMembers, Module)) {
    static if (__traits(compiles, { static assert(isFunctionPointer!(
      typeof(mixin("Module." ~ m)))); }))
    {
      mixin("Module." ~ m) = cast(typeof(mixin("Module." ~ m)))
        g_sharedLib.loadSymbol(m);
    }
  }
}

shared static this() {
  version (linux) {
    auto sslPaths = ["libevent.so"];
  } else version (Win32) {
    auto sslPaths = ["libevent.dll"];
  } else version (darwin) {
    auto sslPaths = ["libevent.dylib"];
  } else version (freebsd) {
    auto sslPaths = ["libevent.so"];
  } else version (solaris) {
    auto sslPaths = ["libevent.so"];
  }
  auto lib = new SharedLib;
  lib.load(sslPaths);
  g_sharedLib = lib;
}

private {
  __gshared SharedLib g_sharedLib;
}
