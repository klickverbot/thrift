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

#
# Common thrift code generation rules
#
d_test_flags = -w -I$(top_srcdir)/lib/d/src -L-L$(top_builddir)/lib/d -L-lthriftd

THRIFT = $(top_builddir)/compiler/cpp/thrift

thrift_test_gen = $(addprefix gen-d/thrift/test/, SecondService.d \
	ThriftTest.d ThriftTest_constants.d ThriftTest_types.d)

$(thrift_test_gen): $(top_srcdir)/test/ThriftTest.thrift
	$(THRIFT) --gen d $<

check-local: client server

client: client.d common.d $(thrift_test_gen)
	$(DMD) $(d_test_flags) -Igen-d -ofclient $(thrift_test_gen) common.d client.d

server: server.d common.d $(thrift_test_gen)
	$(DMD) $(d_test_flags) -Igen-d -ofserver $(thrift_test_gen) common.d server.d

clean-local:
	$(RM) -rf gen-d client client.o server server.o
