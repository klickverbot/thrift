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

d_thriftmodules = $(addprefix thrift/, base codegen hashset)
d_thriftdir = $(D_IMPORT_PREFIX)/thrift
d_thrift_DATA = $(addprefix src/, $(addsuffix .d,$(d_thriftmodules)))

d_protocolmodules = $(addprefix thrift/protocol/, base binary compact json \
	processor)
d_protocoldir = $(d_thriftdir)/protocol
d_protocol_DATA = $(addprefix src/, $(addsuffix .d,$(d_protocolmodules)))

d_servermodules = $(addprefix thrift/server/, base simple threaded)
d_serverdir = $(d_thriftdir)/server
d_server_DATA = $(addprefix src/, $(addsuffix .d,$(d_servermodules)))

d_servertransportmodules = $(addprefix thrift/server/transport/, base socket ssl)
d_servertransportdir = $(d_thriftdir)/server/transport
d_servertransport_DATA = $(addprefix src/, $(addsuffix .d, \
	$(d_servertransportmodules)))

d_transportmodules = $(addprefix thrift/transport/, base buffered file \
	framed http memory socket ssl)
d_transportdir = $(d_thriftdir)/transport
d_transport_DATA = $(addprefix src/, $(addsuffix .d,$(d_transportmodules)))

d_opensslmodules = $(addprefix thrift/util/openssl/, asn1 bio crypto err \
	loader objects rand safestack ssl stack x509 x509_vfy x509v3)
d_openssldir = $(d_thriftdir)/util/openssl
d_openssl_DATA = $(addprefix src/, $(addsuffix .d,$(d_opensslmodules)))

d_modules = $(d_thriftmodules) $(d_protocolmodules) $(d_servermodules) \
	$(d_opensslmodules) $(d_servertransportmodules) $(d_transportmodules)

d_sources = $(addprefix src/, $(addsuffix .d, $(d_modules)))
libthriftd.a: $(d_sources)
	$(DMD) -oflibthriftd -w -lib $(d_sources)

all-local: libthriftd.a

clean-local:
	$(RM) -rf libthriftd.a unittest

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
