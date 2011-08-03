module thrift.c.openssl.asn1;

import thrift.c.openssl.loader;

shared static this() {
  bindFunctions!(thrift.c.openssl.asn1)();
}

__gshared:
nothrow:

alias void ASN1_STRING;
alias ASN1_STRING ASN1_IA5STRING;

alias extern(C) char* function(ASN1_STRING*) ASN1_STRING_data_t;
ASN1_STRING_data_t ASN1_STRING_data;

alias extern(C) int function(ASN1_STRING*) ASN1_STRING_length_t;
ASN1_STRING_length_t ASN1_STRING_length;

alias extern(C) int function(char**, ASN1_STRING*) ASN1_STRING_to_UTF8_t;
ASN1_STRING_to_UTF8_t ASN1_STRING_to_UTF8;
