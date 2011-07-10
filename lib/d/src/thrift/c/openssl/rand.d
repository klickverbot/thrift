module thrift.c.openssl.rand;

import thrift.c.openssl.loader;

shared static this() {
  bindFunctions!(thrift.c.openssl.rand)();
}

__gshared:
nothrow:

alias extern(C) int function() RAND_poll_t;
RAND_poll_t RAND_poll;
