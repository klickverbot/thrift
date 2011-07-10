module thrift.c.openssl.bio;

import core.stdc.config;
import thrift.c.openssl.loader;

shared static this() {
  bindFunctions!(thrift.c.openssl.bio)();
}

__gshared:
nothrow:

alias void BIO;

enum BIO_CTRL_FLUSH = 11;

c_long BIO_flush(BIO* b) {
  return BIO_ctrl(b, BIO_CTRL_FLUSH, 0, null);
}

alias extern(C) c_long function(BIO*, int, c_long, void*) BIO_ctrl_t;
BIO_ctrl_t BIO_ctrl;
