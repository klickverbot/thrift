/**
 * libevent event.h interface (still included with libevent2, but called
 * »compat«).
 */
module thrift.c.event.event;

import core.sys.posix.sys.time;
import thrift.c.event.loader;

shared static this() {
  bindFunctions!(thrift.c.event.event)();
}

__gshared:
nothrow:

version (Windows) {
  alias ptrdiff_t evutil_socket_t;
} else {
  alias int evutil_socket_t;
}

alias extern(C) void function(evutil_socket_t, short, void *) event_callback_fn;

extern(C) struct event {
	tailq_entry_event ev_active_next;
	tailq_entry_event ev_next;
	union ev_timeout_pos_t {
	  tailq_entry_event ev_next_with_common_timeout;
	  int min_heap_idx;
	}
	ev_timeout_pos_t ev_timeout_pos;
	evutil_socket_t ev_fd;

	event_base* ev_base;

	union _ev_t {
		struct ev_io_t {
			tailq_entry_event ev_io_next;
			timeval ev_timeout;
		}
		ev_io_t ev_io;

		struct ev_signal_t {
			tailq_entry_event ev_signal_next;
			short ev_ncalls;
			short *ev_pncalls;
		}
		ev_signal_t ev_signal;
	}
	_ev_t _ev;

	short ev_events;
	short ev_res;
	short ev_flags;
	ubyte ev_pri;
	ubyte ev_closure;
	timeval ev_timeout;

	/* allows us to adopt for different types of events */
	event_callback_fn ev_callback;
	void* ev_arg;
}

extern(C) struct tailq_entry_event {
  event* tqe_next;
  event** tqe_prev;
}

alias void event_base;

enum EV_TIMEOUT = 0x1;
enum EV_READ = 0x2;
enum EV_WRITE = 0x4;
enum EV_SIGNAL = 0x8;
enum EV_PERSIST = 0x10;
enum EV_ET = 0x20;

alias extern(C) int function(event*, const(timeval)*) event_add_t;
event_add_t event_add;

alias extern(C) event_base* function() event_base_new_t;
event_base_new_t event_base_new;

alias extern(C) void function(event_base*) event_base_free_t;
event_base_free_t event_base_free;

alias extern(C) int function(event_base*, evutil_socket_t, short, event_callback_fn, void*, const(timeval)*) event_base_once_t;
event_base_once_t event_base_once;

alias extern(C) int function(event_base*, event*) event_base_set_t;
event_base_set_t event_base_set;

alias extern(C) int function(event_base*, int) event_base_loop_t;
event_base_loop_t event_base_loop;

alias extern(C) int function(event*) event_del_t;
event_del_t event_del;

alias extern(C) void function(event*) event_free_t;
event_free_t event_free;

alias extern(C) event* function(event_base*, evutil_socket_t, short, event_callback_fn, void*) event_new_t;
event_new_t event_new;
