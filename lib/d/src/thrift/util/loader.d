/**
 * Support code for dynamically loading shared libraries.
 *
 * This was originally written by me (David Nadlinger) for the SWIG project,
 * and is based on work by (and used with permission from) Michael Parker for
 * the Derelict project.
 *
 * License: Public Domain (Boost Software License 1.0 where not applicable).
 */
module thrift.util.loader;

import std.conv : to;
import std.string : toStringz;

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

private {
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
    import std.windows.syserror;

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
