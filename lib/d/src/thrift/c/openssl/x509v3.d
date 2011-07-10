module thrift.c.openssl.x509v3;

import thrift.c.openssl.loader;
import thrift.c.openssl.asn1;

shared static this() {
  bindFunctions!(thrift.c.openssl.x509v3)();
}

__gshared:
nothrow:

struct GENERAL_NAME {
  enum GEN_OTHERNAME = 0;
  enum GEN_EMAIL = 1;
  enum GEN_DNS = 2;
  enum GEN_X400 = 3;
  enum GEN_DIRNAME = 4;
  enum GEN_EDIPARTY = 5;
  enum GEN_URI = 6;
  enum GEN_IPADD = 7;
  enum GEN_RID = 8;

  int type;
  union D {
    ASN1_IA5STRING *ia5;/* rfc822Name, dNSName, uniformResourceIdentifier */
  }
  D d;
}

alias extern(C) void function() GENERAL_NAME_free_t;
GENERAL_NAME_free_t GENERAL_NAME_free;
