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

THRIFT = $(top_builddir)/compiler/cpp/thrift

thrift_test_gen = $(addprefix gen-d/, DebugProtoTest_types.d)

$(thrift_test_gen): $(top_srcdir)/test/DebugProtoTest.thrift
	$(THRIFT) --gen d $<

d_test_flags = -w -O -release -inline -I$(top_srcdir)/lib/d/src \
	-L-L$(top_builddir)/lib/d -L-lthriftd $(DMD_LIBDL_FLAGS)

check-local: benchmark

benchmark: $(thrift_test_gen) benchmark.d
	$(DMD) $(d_test_flags) -Igen-d -ofbenchmark $(thrift_test_gen) benchmark.d

clean-local:
	$(RM) -rf gen-d benchmark benchmark.o
