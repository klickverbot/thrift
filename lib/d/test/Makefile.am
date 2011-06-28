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

# General build flags

d_test_flags = -w -O -release -inline -I$(top_srcdir)/lib/d/src -Igen-d \
	-L-L$(top_builddir)/lib/d -L-lthriftd $(DMD_LIBDL_FLAGS)


# Thrift compiler rules

THRIFT = $(top_builddir)/compiler/cpp/thrift

debug_proto_gen = $(addprefix gen-d/, DebugProtoTest_types.d)

$(debug_proto_gen): $(top_srcdir)/test/DebugProtoTest.thrift
	$(THRIFT) --gen d $<

stress_test_gen = $(addprefix gen-d/thrift/test/stress/, Service.d \
	StressTest_types.d)

$(stress_test_gen): $(top_srcdir)/test/StressTest.thrift
	$(THRIFT) --gen d $<

thrift_test_gen = $(addprefix gen-d/thrift/test/, SecondService.d \
	ThriftTest.d ThriftTest_constants.d ThriftTest_types.d)

$(thrift_test_gen): $(top_srcdir)/test/ThriftTest.thrift
	$(THRIFT) --gen d $<


# The actual test targets.

targets = serialization_benchmark stress_test_server \
	thrift_test_client thrift_test_server

serialization_benchmark: %: %.d $(debug_proto_gen)
	$(DMD) $(d_test_flags) -of$@ $^

stress_test_server: %: %.d $(stress_test_gen)
	$(DMD) $(d_test_flags) -of$@ $^

thrift_test_client thrift_test_server: %: %.d thrift_test_common.d $(thrift_test_gen)
	$(DMD) $(d_test_flags) -of$@ $^

check-local: $(targets)

clean-local:
	$(RM) -rf gen-d $(targets) $(addsuffix .o, $(targets))
