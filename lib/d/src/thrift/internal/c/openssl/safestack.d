module thrift.internal.c.openssl.safestack;

import thrift.internal.c.openssl.x509v3;
import thrift.internal.c.openssl.stack;

int sk_GENERAL_NAME_num(STACK* st) {
  return sk_num(st);
}

GENERAL_NAME* sk_GENERAL_NAME_value(STACK* st, int i) {
  return cast(GENERAL_NAME*) sk_value(st, i);
}

void sk_GENERAL_NAME_pop_free(STACK* st, pop_free_func func) {
  sk_pop_free(st, func);
}
