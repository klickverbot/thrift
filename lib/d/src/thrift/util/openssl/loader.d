module thrift.util.openssl.loader;

import std.conv : to;
import std.string : toStringz;
import std.traits : isFunctionPointer;
import thrift.util.loader;

enum Library {
  crypto,
  ssl
}

void bindFunctions(alias Module, Library library = Library.crypto)() {
  foreach (m; __traits(derivedMembers, Module)) {
    static if (__traits(compiles, { static assert(isFunctionPointer!(
      typeof(mixin("Module." ~ m)))); }))
    {
      mixin("Module." ~ m) = cast(typeof(mixin("Module." ~ m)))
        g_sharedLibs[library].loadSymbol(m);
    }
  }
}

shared static this() {
  version (linux) {
    auto sslPaths = ["libssl.so.0.9.8", "libssl.so"];
  } else version (Win32) {
    auto sslPaths = ["ssleay32.dll"];
  } else version (darwin) {
    auto sslPaths = ["/usr/lib/libssl.dylib", "libssl.dylib"];
  } else version (freebsd) {
    auto sslPaths = ["libssl.so.5", "libssl.so"];
  } else version (solaris) {
    auto sslPaths = ["libssl.so.0.9.8", "libssl.so"];
  }
  auto lib = new SharedLib;
  lib.load(sslPaths);
  g_sharedLibs[Library.ssl] = lib;

  version (darwin) {
    auto cryptoLib = new SharedLib;
    cryptoLib.load(["/usr/lib/libcrypto.dylib", "libcrypto.dylib"]);
    g_sharedLibs[Library.crypto] = cryptoLib;
  } else version (Win32) {
    auto cryptoLib = new SharedLib;
    cryptoLib.load(["libeay32.dll"]);
    g_sharedLibs[Library.crypto] = cryptoLib;
  } else {
    g_sharedLibs[Library.crypto] = g_sharedLibs[Library.ssl];
  }
}

private {
  __gshared SharedLib[Library.max + 1] g_sharedLibs;
}
