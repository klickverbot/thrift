#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements. See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership. The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License. You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied. See the License for the
# specific language governing permissions and limitations
# under the License.
#

SUBDIRS = test

d_thriftmodules = $(addprefix thrift/, base)
d_thriftdir = $(D_IMPORT_PREFIX)/thrift
d_thrift_DATA = $(addprefix src/, $(addsuffix .d, $(d_thriftmodules)))

d_asyncmodules = $(addprefix thrift/async/, base libevent socket)
d_asyncdir = $(d_thriftdir)/async
d_async_DATA = $(addprefix src/, $(addsuffix .d, $(d_asyncmodules)))

d_codegenmodules = $(addprefix thrift/codegen/, async_client \
 	async_client_pool base client client_pool processor)
d_codegendir = $(d_thriftdir)/codegen
d_codegen_DATA = $(addprefix src/, $(addsuffix .d, $(d_codegenmodules)))

d_protocolmodules = $(addprefix thrift/protocol/, base binary compact json \
	processor)
d_protocoldir = $(d_thriftdir)/protocol
d_protocol_DATA = $(addprefix src/, $(addsuffix .d, $(d_protocolmodules)))

d_servermodules = $(addprefix thrift/server/, base simple nonblocking \
	taskpool threaded)
d_serverdir = $(d_thriftdir)/server
d_server_DATA = $(addprefix src/, $(addsuffix .d, $(d_servermodules)))

d_servertransportmodules = $(addprefix thrift/server/transport/, base socket ssl)
d_servertransportdir = $(d_thriftdir)/server/transport
d_servertransport_DATA = $(addprefix src/, $(addsuffix .d, \
	$(d_servertransportmodules)))

d_transportmodules = $(addprefix thrift/transport/, base buffered file \
	framed http memory piped range socket ssl zlib)
d_transportdir = $(d_thriftdir)/transport
d_transport_DATA = $(addprefix src/, $(addsuffix .d, $(d_transportmodules)))

d_utilmodules = $(addprefix thrift/util/, awaitable cancellation future \
	hashset resource_pool)
d_utildir = $(d_thriftdir)/util
d_util_DATA = $(addprefix src/, $(addsuffix .d, $(d_utilmodules)))

d_internalmodules = $(addprefix thrift/internal/, algorithm ctfe endian \
	socket traits)
d_internaldir = $(d_thriftdir)/internal
d_internal_DATA = $(addprefix src/, $(addsuffix .d, $(d_internalmodules)))

d_cmodules = $(addprefix thrift/internal/c/, loader)
d_cdir = $(d_internaldir)/c
d_c_DATA = $(addprefix src/, $(addsuffix .d, $(d_cmodules)))

d_eventmodules = $(addprefix thrift/internal/c/event/, event event_compat loader)
d_eventdir = $(d_cdir)/event
d_event_DATA = $(addprefix src/, $(addsuffix .d, $(d_eventmodules)))

d_opensslmodules = $(addprefix thrift/internal/c/openssl/, asn1 bio crypto err \
	loader objects rand safestack ssl stack x509 x509_vfy x509v3)
d_openssldir = $(d_cdir)/openssl
d_openssl_DATA = $(addprefix src/, $(addsuffix .d, $(d_opensslmodules)))


d_publicmodules = $(d_thriftmodules) $(d_asyncmodules) \
	$(d_codegenmodules) $(d_protocolmodules) $(d_servermodules) \
	$(d_servertransportmodules) $(d_transportmodules) $(d_utilmodules)
d_publicsources = $(addprefix src/, $(addsuffix .d, $(d_publicmodules)))

d_modules = $(d_publicmodules) \
	$(d_internalmodules) $(d_cmodules) $(d_eventmodules) $(d_opensslmodules)

d_sources = $(addprefix src/, $(addsuffix .d, $(d_modules)))
libthriftd.a: $(d_sources)
	$(DMD) -oflibthriftd -w -wi -lib $^

docs: $(d_publicsources) src/thrift/index.d
	dil ddoc docs -hl --kandil $^

all-local: libthriftd.a

clean-local:
	$(RM) -rf docs libthriftd.a unittest

lib_LIBRARIES = libthriftd.a


#
# Unit tests
#
d_test_flags = -w -gc -I$(top_srcdir)/lib/d/src -L-L$(top_builddir)/lib/d \
	-L-lthriftd $(DMD_LIBDL_FLAGS)

unittest/emptymain.d: unittest/.directory
	@echo 'void main(){}' >$@

unittest/.directory:
	mkdir -p unittest || exists unittest
	touch $@

unittest/%: src/%.d libthriftd.a unittest/emptymain.d
	$(DMD) -unittest -of$@ $(d_test_flags) unittest/emptymain.d $<

TESTS = $(addprefix unittest/, $(d_modules))


EXTRA_DIST = \
	README
