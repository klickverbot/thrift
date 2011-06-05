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

SUBDIRS = test/ThriftTest

d_thriftdir = $(D_IMPORT_PREFIX)/thrift
d_thrift_DATA = \
	src/thrift/base.d \
	src/thrift/codegen.d \
	src/thrift/hashset.d

d_protocoldir = $(d_thriftdir)/protocol
d_protocol_DATA = \
	src/thrift/protocol/base.d \
	src/thrift/protocol/binary.d \
	src/thrift/protocol/processor.d

d_serverdir = $(d_thriftdir)/server
d_server_DATA = \
	src/thrift/server/base.d \
	src/thrift/server/simple.d

d_transportdir = $(d_thriftdir)/transport
d_transport_DATA = \
	src/thrift/transport/base.d \
	src/thrift/transport/buffered.d \
	src/thrift/transport/server.d \
	src/thrift/transport/serversocket.d \
	src/thrift/transport/socket.d

d_sources = $(d_thrift_DATA) $(d_protocol_DATA) $(d_server_DATA) $(d_transport_DATA)

all-local: libthriftd.a

libthriftd.a: $(d_sources)
	$(DMD) -oflibthriftd -w -lib $(d_sources)

clean-local:
	$(RM) libthriftd.a unittest emptymain.d

lib_LIBRARIES = libthriftd.a

TESTS = unittest

emptymain.d:
	@echo 'void main() {}' >$@

unittest: $(d_sources) emptymain.d libthriftd.a
	$(DMD) -unittest -w -ofunittest -Isrc $(d_sources) emptymain.d

EXTRA_DIST = \
	README
