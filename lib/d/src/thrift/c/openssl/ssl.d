module thrift.c.openssl.ssl;

import core.stdc.config;
import thrift.c.openssl.bio;
import thrift.c.openssl.loader;
import thrift.c.openssl.x509;
import thrift.c.openssl.x509_vfy;

shared static this() {
  bindFunctions!(thrift.c.openssl.ssl, Library.ssl)();
}

c_long SSL_CTX_set_mode(SSL_CTX* ctx, c_long op) {
  return SSL_CTX_ctrl(ctx, SSL_CTRL_MODE, op, null);
}

__gshared:
nothrow:

alias void SSL;
alias void SSL_CTX;
alias void SSL_METHOD;

enum SSL_CTRL_MODE = 33;

enum SSL_MODE_AUTO_RETRY = 0x00000004L;

enum SSL_SENT_SHUTDOWN = 1;
enum SSL_RECEIVED_SHUTDOWN = 2;

enum SSL_VERIFY_NONE = 0x00;
enum SSL_VERIFY_PEER = 0x01;
enum SSL_VERIFY_FAIL_IF_NO_PEER_CERT = 0x02;
enum SSL_VERIFY_CLIENT_ONCE = 0x04;

alias X509_FILETYPE_PEM SSL_FILETYPE_PEM;

alias extern(C) int function(char *buf, int size, int rwflag, void *userdata) pem_password_cb;

alias extern(C) int function(SSL*) SSL_accept_t;
SSL_accept_t SSL_accept;

alias extern(C) int function(SSL*) SSL_connect_t;
SSL_connect_t SSL_connect;

alias extern(C) c_long function(SSL_CTX*, int, c_long, void*) SSL_CTX_ctrl_t;
SSL_CTX_ctrl_t SSL_CTX_ctrl;

alias extern(C) void function(SSL_CTX*) SSL_CTX_free_t;
SSL_CTX_free_t SSL_CTX_free;

alias extern(C) int function(SSL_CTX*, const(char)*, const(char)*) SSL_CTX_load_verify_locations_t;
SSL_CTX_load_verify_locations_t SSL_CTX_load_verify_locations;

alias extern(C) SSL_CTX* function(SSL_METHOD*) SSL_CTX_new_t;
SSL_CTX_new_t SSL_CTX_new;

alias extern(C) int function(SSL_CTX*, const(char)*) SSL_CTX_set_cipher_list_t;
SSL_CTX_set_cipher_list_t SSL_CTX_set_cipher_list;

alias extern(C) void function(SSL_CTX*, pem_password_cb) SSL_CTX_set_default_passwd_cb_t;
SSL_CTX_set_default_passwd_cb_t SSL_CTX_set_default_passwd_cb;

alias extern(C) void function(SSL_CTX*, void*) SSL_CTX_set_default_passwd_cb_userdata_t;
SSL_CTX_set_default_passwd_cb_userdata_t SSL_CTX_set_default_passwd_cb_userdata;

alias extern(C) void function(SSL_CTX*, int, int function(int, X509_STORE_CTX*)) SSL_CTX_set_verify_t;
SSL_CTX_set_verify_t SSL_CTX_set_verify;

alias extern(C) int function(SSL_CTX*, const(char)*) SSL_CTX_use_certificate_chain_file_t;
SSL_CTX_use_certificate_chain_file_t SSL_CTX_use_certificate_chain_file;

alias extern(C) int function(SSL_CTX*, const(char)*, int) SSL_CTX_use_PrivateKey_file_t;
SSL_CTX_use_PrivateKey_file_t SSL_CTX_use_PrivateKey_file;

alias extern(C) void function(SSL*) SSL_free_t;
SSL_free_t SSL_free;

alias extern(C) int function(const SSL*, int) SSL_get_error_t;
SSL_get_error_t SSL_get_error;

alias extern(C) X509* function(const SSL*) SSL_get_peer_certificate_t;
SSL_get_peer_certificate_t SSL_get_peer_certificate;

alias extern(C) int function(const SSL*) SSL_get_shutdown_t;
SSL_get_shutdown_t SSL_get_shutdown;

alias extern(C) int function(const SSL*) SSL_get_verify_mode_t;
SSL_get_verify_mode_t SSL_get_verify_mode;

alias extern(C) c_long function(const SSL*) SSL_get_verify_result_t;
SSL_get_verify_result_t SSL_get_verify_result;

alias extern(C) BIO* function(const SSL*) SSL_get_wbio_t;
SSL_get_wbio_t SSL_get_wbio;

alias extern(C) int function() SSL_library_init_t;
SSL_library_init_t SSL_library_init;

alias extern(C) void function() SSL_load_error_strings_t;
SSL_load_error_strings_t SSL_load_error_strings;

alias extern(C) SSL* function(SSL_CTX*) SSL_new_t;
SSL_new_t SSL_new;

alias extern(C) int function(SSL*, void*, int) SSL_peek_t;
SSL_peek_t SSL_peek;

alias extern(C) int function(SSL*, void*, int) SSL_read_t;
SSL_read_t SSL_read;

alias extern(C) int function(SSL*, int) SSL_set_fd_t;
SSL_set_fd_t SSL_set_fd;

alias extern(C) int function(SSL*) SSL_shutdown_t;
SSL_shutdown_t SSL_shutdown;

alias extern(C) int function(SSL*, const void*, int) SSL_write_t;
SSL_write_t SSL_write;

alias extern(C) SSL_METHOD* function() TLSv1_method_t;
TLSv1_method_t TLSv1_method;
