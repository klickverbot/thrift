module thrift.internal.c.openssl.crypto;

import core.stdc.config;
import thrift.internal.c.openssl.loader;

shared static this() {
  bindFunctions!(thrift.internal.c.openssl.crypto)();
}

__gshared:
nothrow:

alias void CRYPTO_dynlock_value;

enum CRYPTO_LOCK = 1;
enum CRYPTO_UNLOCK = 2;

alias extern(C) void function() CRYPTO_cleanup_all_ex_data_t;
CRYPTO_cleanup_all_ex_data_t CRYPTO_cleanup_all_ex_data;

alias extern(C) void function(void*) CRYPTO_free_t;
CRYPTO_free_t CRYPTO_free;

alias extern(C) int function() CRYPTO_num_locks_t;
CRYPTO_num_locks_t CRYPTO_num_locks;

alias extern(C) void function(CRYPTO_dynlock_value* function(const(char)*, int)) CRYPTO_set_dynlock_create_callback_t;
CRYPTO_set_dynlock_create_callback_t CRYPTO_set_dynlock_create_callback;

alias extern(C) void function(void function(int, CRYPTO_dynlock_value*, const(char)*, int)) CRYPTO_set_dynlock_lock_callback_t;
CRYPTO_set_dynlock_lock_callback_t CRYPTO_set_dynlock_lock_callback;

alias extern(C) void function(void function(CRYPTO_dynlock_value*, const(char)*, int)) CRYPTO_set_dynlock_destroy_callback_t;
CRYPTO_set_dynlock_destroy_callback_t CRYPTO_set_dynlock_destroy_callback;

alias extern(C) void function(void function(int, int, const(char)*, int)) CRYPTO_set_locking_callback_t;
CRYPTO_set_locking_callback_t CRYPTO_set_locking_callback;

alias extern(C) void function(c_ulong function()) CRYPTO_set_id_callback_t;
CRYPTO_set_id_callback_t CRYPTO_set_id_callback;
