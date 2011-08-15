module thrift.internal.c.openssl.rand;

import thrift.internal.c.openssl.loader;

shared static this() {
  bindFunctions!(thrift.internal.c.openssl.rand)();
}

__gshared:
nothrow:

alias extern(C) int function() RAND_poll_t;
RAND_poll_t RAND_poll;
