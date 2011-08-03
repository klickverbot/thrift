/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
module async_test_common;

import thrift.base;
import thrift.codegen;

interface AsyncTest {
  string echo(string value);
  string delayedEcho(string value, long milliseconds);

  void fail(string reason);
  void delayedFail(string reason, long milliseconds);

  enum methodMeta = [
    TMethodMeta("fail", [], [TExceptionMeta("ate", 1, "AsyncTestException")]),
    TMethodMeta("delayedFail", [], [TExceptionMeta("ate", 1, "AsyncTestException")])
  ];
  alias .AsyncTestException AsyncTestException;
}

class AsyncTestException : TException {
  string reason;
  mixin TStructHelpers!();
}
