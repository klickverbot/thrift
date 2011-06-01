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
THRIFT = $(top_builddir)/compiler/cpp/thrift

thrift_test_gen = \
  gen-d/thrift/test/SecondService.d \
  gen-d/thrift/test/ThriftTest.d \
  gen-d/thrift/test/ThriftTest_constants.d \
  gen-d/thrift/test/ThriftTest_types.d

$(thrift_test_gen): $(top_srcdir)/test/ThriftTest.thrift
	$(THRIFT) --gen d $<

d_flags = -I$(top_srcdir)/lib/d/src -Igen-d -L-L$(top_builddir)/lib/d -L-lthriftd

check-local: client server

client: client.d common.d $(thrift_test_gen)
	$(DMD) $(d_flags) -ofclient $(thrift_test_gen) common.d client.d

server: server.d common.d $(thrift_test_gen)
	$(DMD) $(d_flags) -ofserver $(thrift_test_gen) common.d client.d

clean-local:
	$(RM) -rf gen-d client client.o server server.o
