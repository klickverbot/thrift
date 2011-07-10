/**
 * libevent1 event.h interface (still included with libevent2, but called
 * »compat«).
 */
module thrift.c.event.event_compat;

import thrift.c.event.event;
import thrift.c.event.loader;

shared static this() {
  bindFunctions!(thrift.c.event.event_compat)();
}

__gshared:
nothrow:

alias extern(C) event_base* function() event_init_t;
event_init_t event_init;

alias extern(C) const(char)* function() event_get_method_t;
event_get_method_t event_get_method;

alias extern(C) const(char)* function() event_get_version_t;
event_get_version_t event_get_version;

alias extern(C) void function(event*, evutil_socket_t, short, void function(evutil_socket_t, short, void*), void*) event_set_t;
event_set_t event_set;
