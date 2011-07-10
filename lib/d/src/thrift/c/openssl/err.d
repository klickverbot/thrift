module thrift.c.openssl.err;

import core.stdc.config;
import thrift.c.openssl.loader;

shared static this() {
  bindFunctions!(thrift.c.openssl.err)();
}

__gshared:
nothrow:

enum SSL_ERROR_SYSCALL = 5;

alias extern(C) void function() ERR_clear_error_t;
ERR_clear_error_t ERR_clear_error;

alias extern(C) void function() ERR_free_strings_t;
ERR_free_strings_t ERR_free_strings;

alias extern(C) c_ulong function() ERR_get_error_t;
ERR_get_error_t ERR_get_error;

alias extern(C) c_ulong function() ERR_peek_error_t;
ERR_peek_error_t ERR_peek_error;

alias extern(C) const(char)* function(c_ulong) ERR_reason_error_string_t;
ERR_reason_error_string_t ERR_reason_error_string;

alias extern(C) void function(c_ulong) ERR_remove_state_t;
ERR_remove_state_t ERR_remove_state;
