module thrift.internal.c.openssl.x509;

import core.stdc.config;
import thrift.internal.c.openssl.asn1;
import thrift.internal.c.openssl.loader;

shared static this() {
  bindFunctions!(thrift.internal.c.openssl.x509)();
}

__gshared:
nothrow:

enum X509_FILETYPE_PEM = 1;

alias void X509;
alias void X509_NAME;
alias void X509_NAME_ENTRY;

alias extern(C) const(char)* function(c_long) X509_verify_cert_error_string_t;
X509_verify_cert_error_string_t X509_verify_cert_error_string;

alias extern(C) void* function(X509*, int, int*, int*) X509_get_ext_d2i_t;
X509_get_ext_d2i_t X509_get_ext_d2i;

alias extern(C) ASN1_STRING* function(X509_NAME_ENTRY*) X509_NAME_ENTRY_get_data_t;
X509_NAME_ENTRY_get_data_t X509_NAME_ENTRY_get_data;

alias extern(C) X509_NAME_ENTRY* function(X509_NAME*, int) X509_NAME_get_entry_t;
X509_NAME_get_entry_t X509_NAME_get_entry;

alias extern(C) int function(X509_NAME*, int, int) X509_NAME_get_index_by_NID_t;
X509_NAME_get_index_by_NID_t X509_NAME_get_index_by_NID;

alias extern(C) X509_NAME* function(X509*) X509_get_subject_name_t;
X509_get_subject_name_t X509_get_subject_name;

alias extern(C) void function(X509*) X509_free_t;
X509_free_t X509_free;
