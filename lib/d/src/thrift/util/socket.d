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

/**
 * Contains abstractions for OS-dependent socket functionality for internal
 * use.
 */
module thrift.util.socket;

import std.conv : to;

// FreeBSD and OS X return -1 and set ECONNRESET if socket was closed by
// the other side, we need to check for that before throwing an exception.
version (FreeBSD) {
  enum connresetOnPeerShutdown = true;
} else version (OSX) {
  enum connresetOnPeerShutdown = true;
} else {
  enum connresetOnPeerShutdown = false;
}

version (Win32) {
  import std.c.windows.winsock : WSAGetLastError, WSAEINTR;
  import std.windows.syserror : sysErrorString;
} else {
  import core.stdc.errno : getErrno, EAGAIN, ECONNRESET, EINTR, EWOULDBLOCK;
  import core.stdc.string : strerror;
}

version (Win32) {
  alias WSAGetLastError getSocketErrno;
  enum INTERRUPTED_ERRNO = WSAEINTR;
  // See http://msdn.microsoft.com/en-us/library/ms740668.aspx.
  enum TIMEOUT_ERRNO = 10060;
} else {
  alias getErrno getSocketErrno;
  alias EINTR INTERRUPTED_ERRNO;

  // TODO: The C++ TSocket implementation mentions that EAGAIN can also be
  // set (undocumentedly) in out of resource conditions; adapt the code
  // accordingly.
  alias EAGAIN TIMEOUT_ERRNO;
}

string socketErrnoString(uint errno) {
  version (Win32) {
    return sysErrorString(errno);
  } else {
    return to!string(strerror(errno));
  }
}
