/**
 * Support code for dynamically loading shared libraries, adapted for loading
 * OpenSSL.
 *
 * The loading code was adapted from the Derelict project and is used with
 * permission from Michael Parker, the original author.
 *
 * Author: David Nadlinger
 *
 * License: Public Domain (Boost Software License 1.0 where not applicable).
 */
module thrift.util.openssl.loader;

import std.conv : to;
import std.string : toStringz;
import std.traits : isFunctionPointer;

enum Library {
  ssl,
  crypto
}

void bindFunctions(alias Module, Library library = Library.ssl)() {
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
    auto sslPaths = ["libssl32.dll"];
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

class SharedLibLoadException : Exception {
  this(in string[] libNames, in string[] reasons, string file = __FILE__,
    size_t line = __LINE__, Throwable next = null)
  {
    string msg = "Failed to load one or more shared libraries:";
    foreach(i, n; libNames) {
      msg ~= "\n\t" ~ n ~ " - ";
      if(i < reasons.length)
        msg ~= reasons[i];
      else
        msg ~= "Unknown";
    }
    super(msg, file, line, next);
  }
}

class SymbolLoadException : Exception {
  this(string sharedLibName, string symbolName, string file = __FILE__,
    size_t line = __LINE__, Throwable next = null)
  {
    super("Failed to load symbol " ~ symbolName ~ " from shared library " ~
    sharedLibName, file, line, next);
    symbolName_ = symbolName;
  }

  string symbolName() {
    return symbolName_;
  }

private:
  string symbolName_;
}

private {
  __gshared SharedLib[Library.max + 1] g_sharedLibs;

  final class SharedLib {
    void load(string[] names) {
      if (hlib_ !is null) return;

      string[] failedLibs;
      string[] reasons;

      foreach(n; names) {
        hlib_ = loadSharedLib(n);
        if (hlib_ is null) {
          failedLibs ~= n;
          reasons ~= getErrorStr();
          continue;
        }
        name_ = n;
        break;
      }

      if (hlib_ is null) {
        throw new SharedLibLoadException(failedLibs, reasons);
      }
    }

    void* loadSymbol(string symbolName, bool doThrow = true) {
      void* sym = getSymbol(hlib_, symbolName);
      if(doThrow && (sym is null)) {
        throw new SymbolLoadException(name_, symbolName);
      }
      return sym;
    }

    void unload() {
      if(hlib_ !is null) {
        unloadSharedLib(hlib_);
        hlib_ = null;
      }
    }

  private:
    string name_;
    SharedLibHandle hlib_;
  }

  version (Posix) {
    version (freebsd) {} else {
      pragma(lib, "dl");
    }

    import core.sys.posix.dlfcn;
    alias void* SharedLibHandle;

    SharedLibHandle loadSharedLib(string libName) {
      return dlopen(toStringz(libName), RTLD_NOW);
    }

    void unloadSharedLib(SharedLibHandle hlib) {
      dlclose(hlib);
    }

    void* getSymbol(SharedLibHandle hlib, string symbolName) {
      return dlsym(hlib, toStringz(symbolName));
    }

    string getErrorStr() {
      auto err = dlerror();
      if (err is null) {
        return "Unknown Error";
      }
      return to!string(err);
    }
  } else version (Windows) {
    import core.sys.windows.windows;
    import std.windows.syserror.d;

    alias HMODULE SharedLibHandle;

    SharedLibHandle loadSharedLib(string libName) {
      return LoadLibraryA(toStringz(libName));
    }

    void unloadSharedLib(SharedLibHandle hlib) {
      FreeLibrary(hlib);
    }

    void* getSymbol(SharedLibHandle hlib, string symbolName) {
      return GetProcAddress(hlib, toStringz(symbolName));
    }

    string getErrorStr() {
      return sysErrorString(GetLastError());
    }
  } else {
    static assert(false, "OS not supported by the library loading code.");
  }
}