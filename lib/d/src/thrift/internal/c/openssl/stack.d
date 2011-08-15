module thrift.internal.c.openssl.stack;

import thrift.internal.c.openssl.loader;

shared static this() {
  bindFunctions!(thrift.internal.c.openssl.stack)();
}

__gshared:
nothrow:

alias void STACK;

alias extern(C) int function(const STACK *) sk_num_t;
sk_num_t sk_num;

alias extern(C) void function() pop_free_func;
alias extern(C) void function(STACK*, pop_free_func) sk_pop_free_t;
sk_pop_free_t sk_pop_free;

alias extern(C) char* function(const STACK*, int) sk_value_t;
sk_value_t sk_value;
