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
module thrift.internal.test.server;

import core.sync.condition;
import core.sync.mutex;
import core.thread;
import std.datetime;
import std.typecons;
import thrift.protocol.processor;
import thrift.server.base;
import thrift.util.cancellation;

version(unittest):

void testServeCancel(Server)() if (is(Server : TServer)) {
  auto server = new Server(new WhiteHole!TProcessor, 0);
  auto cancel = new TCancellationOrigin;

  auto doneMutex = new Mutex;
  auto doneCondition = new Condition(doneMutex);

  auto serverThread = new Thread({
    server.serve(cancel);
    synchronized (doneMutex) {
      doneCondition.notifyAll();
    }
  });
  serverThread.isDaemon = true;
  serverThread.start();

  Thread.sleep(dur!"msecs"(50));
  cancel.trigger();
  synchronized (doneMutex) {
    assert(doneCondition.wait(dur!"msecs"(10)));
  }
}
