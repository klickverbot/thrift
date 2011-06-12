module thrift.util.openssl.rand;

import thrift.util.openssl.loader;

shared static this() {
  bindFunctions!(thrift.util.openssl.rand);
}

__gshared:
nothrow:

alias extern(C) int function() RAND_poll_t;
RAND_poll_t RAND_poll;
