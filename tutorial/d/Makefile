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

THRIFT_DIR = ../../lib/d/src
LIB_DIR = .

GEN_SRC = ../gen-d/share/SharedService.d ../gen-d/share/shared_types.d ../gen-d/tutorial/tutorial_types.d ../gen-d/tutorial/Calculator.d

default: server client

server: server.d
	dmd -I${THRIFT_DIR} -I../gen-cpp -L-L${LIB_DIR} -L-lthriftd server.d ${GEN_SRC}

client: client.d
	dmd -I${THRIFT_DIR} -I../gen-cpp -L-L${LIB_DIR} -L-lthriftd client.d ${GEN_SRC}

clean:
	$(RM) -r server client
