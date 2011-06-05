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
module thrift.transport.base;

import core.stdc.string : strerror;
import std.conv : to;
import thrift.base;

/**
 * Generic interface for a method of transporting data. A TTransport may be
 * capable of either reading or writing, but not necessarily both.
 */
class TTransport {
  /**
   * Whether this transport is open.
   */
  bool isOpen() @property {
    return false;
  }

  /**
   * Tests whether there is more data to read or if the remote side is
   * still open.
   *
   * By default this is true whenever the transport is open, but
   * implementations should add logic to test for this condition where
   * possible (e.g. on a socket). This is used by a server to check if it
   * should listen for another request.
   */
  bool peek() {
    return isOpen;
  }

  /**
   * Opens the transport for communications.
   *
   * Throws: TTransportException if opening fails.
   */
  void open() {
    throw new TTransportException(TTransportException.Type.NOT_OPEN,
      "Cannot open base TTransport.");
  }

  /**
   * Closes the transport.
   *
   * Throws: TTransportException if closing fails.
   */
  void close() {
    throw new TTransportException(TTransportException.Type.NOT_OPEN,
      "Cannot close base TTransport.");
  }

  /**
   * Attempts to read data into the given buffer, stopping when the buffer is
   * exhausted or no more data is available.
   *
   * The transport must be open when calling this.
   *
   * Params:
   *   buf = Slice to use as buffer.
   *
   * Returns: How many bytes were actually read
   *
   * Throws: TTransportException if an error occurs.
   */
  size_t read(ubyte[] buf) in {
    // DMD @@BUG6108@@: Uncommenting the assertion leads to crashing
    // thrift.transport.serversocket unittests.
    // assert(isOpen, "Called read() on non-open transport!");
  } body {
    throw new TTransportException(TTransportException.Type.NOT_OPEN,
      "Base TTransport cannot read.");
  }

  /**
   * Fills the given buffer by reading data into it, failing if not enough
   * data is available.
   *
   * The transport must be open when calling this.
   *
   * Params:
   *   buf = Slice to use as buffer.
   *
   * Throws: TTransportException if insufficient data is available or reading
   *   fails altogether.
   */
  void readAll(ubyte[] buf) in {
    assert(isOpen, "Called readAll() on non-open transport!");
  } body {
    size_t have;
    while (have < buf.length) {
      size_t get = read(buf[have..$]);
      if (get <= 0) {
        throw new TTransportException(TTransportException.Type.END_OF_FILE,
          "No more data to read.");
      }
      have += get;
    }
  }

  /**
   * Must be called by clients when read is completed.
   *
   * Implementations can choose to perform a transport-specific action, e.g.
   * logging the request to a file.
   *
   * The transport must be open when calling this.
   *
   * Returns: The number of bytes read if available, 0 otherwise.
   */
  size_t readEnd() in {
    assert(isOpen, "Called readEnd() on non-open transport!");
  } body {
    // default behaviour is to do nothing
    return 0;
  }

  /**
   * Writes the passed slice of data.
   *
   * The transport must be open when calling this.
   *
   * Note: You must call flush() to ensure the data is actually written,
   * and available to be read back in the future.  Destroying a TTransport
   * object does not automatically flush pending data – if you destroy a
   * TTransport object with written but unflushed data, that data may be
   * discarded.
   *
   * Params:
   *   buf = Slice of data to write.
   *
   * Throws: TTransportException if an error occurs.
   */
  void write(in ubyte[] buf) in {
    assert(isOpen, "Called write() on non-open transport!");
  } body {
    throw new TTransportException(TTransportException.Type.NOT_OPEN,
      "Base TTransport cannot write.");
  }

  /**
   * Must be called by clients when write is completed.
   *
   * Implementations can choose to perform a transport-specific action, e.g.
   * logging the request to a file.
   *
   * Returns: The number of bytes written if available, 0 otherwise.
   */
  size_t writeEnd() in {
    assert(isOpen, "Called writeEnd() on non-open transport!");
  } body {
    // default behaviour is to do nothing
    return 0;
  }

  /**
   * Flushes any pending data to be written.
   *
   * Must be called before destruction to ensure writes are actually complete,
   * otherwise pending data may be discarded. Typically used with buffered
   * transport mechanisms.
   *
   * The transport must be open when calling this.
   *
   * Throws: TTransportException if an error occurs.
   */
  void flush() in {
    assert(isOpen, "Called flush() on non-open transport!");
  } body {
    // default behaviour is to do nothing
  }

  /**
   * Attempts to return a slice of <code>len</code> bytes of incoming data,
   * possibly copied into buf, not consuming them (i.e.: a later read will
   * return the same data).
   *
   * This method is meant to support protocols that need to read variable-
   * length fields. They can attempt to borrow the maximum amount of data that
   * they will need, then <code>consume()</code> what they actually use. Some
   * transports will not support this method and others will fail occasionally,
   * so protocols must be prepared to fall back to <code>read()</code> if
   * borrow fails.
   *
   * The transport must be open when calling this.
   *
   * Params:
   *   buf = A buffer where the data can be stored if needed, or null to
   *     indicate that the caller is not supplying storage, but would like a
   *     slice of an internal buffer, if available.
   *   len = The number of bytes to borrow.
   *
   * Returns: If the borrow succeeds, a slice containing the borrowed data,
   *   null otherwise. The slice will be at least as long as requested, but
   *   may be longer if the returned slice points into an internal buffer
   *   rather than buf.
   *
   * Throws: TTransportException if an error occurs.
   */
  const(ubyte)[] borrow(ubyte* buf, size_t len) in {
    assert(isOpen, "Called borrow() on non-open transport!");
  } out (result) {
    assert(result is null || result.length >= len,
      "Buffer returned by borrow() too short.");
  } body {
    return null;
  }

  /**
   * Remove len bytes from the transport. This should always follow a borrow
   * of at least len bytes, and should always succeed.
   *
   * The transport must be open when calling this.
   *
   * Params:
   *   len = Number of bytes to consume.
   *
   * Throws: TTransportException if an error occurs.
   */
  void consume(size_t len) in {
    assert(isOpen, "Called consume() on non-open transport!");
  } body {
    throw new TTransportException(TTransportException.Type.NOT_OPEN,
      "Base TTransport cannot consume.");
  }

protected:
  this() {}
}

/**
 * Generic factory class to make an input and output transport out of a
 * source transport. Commonly used inside servers to make input and output
 * streams out of raw clients.
 */
class TTransportFactory {
  /**
   * Default implementation does nothing, just returns the transport given.
   */
  TTransport getTransport(TTransport trans) {
    return trans;
  }
}

class TTransportException : TException {
  /**
   * Error codes for the various types of exceptions.
   */
  enum Type {
    UNKNOWN,
    NOT_OPEN,
    TIMED_OUT,
    END_OF_FILE,
    INTERRUPTED,
    BAD_ARGS,
    CORRUPTED_DATA,
    INTERNAL_ERROR
  }

  this(Type type, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
    string msgForType(Type type) {
      switch (type) {
        case Type.UNKNOWN: return "TTransportException: Unknown transport exception";
        case Type.NOT_OPEN: return "TTransportException: Transport not open";
        case Type.TIMED_OUT: return "TTransportException: Timed out";
        case Type.END_OF_FILE: return "TTransportException: End of file";
        case Type.INTERRUPTED: return "TTransportException: Interrupted";
        case Type.BAD_ARGS: return "TTransportException: Invalid arguments";
        case Type.CORRUPTED_DATA: return "TTransportException: Corrupted Data";
        case Type.INTERNAL_ERROR: return "TTransportException: Internal error";
        default: return "TTransportException: (Invalid exception type)";
      }
    }
    this(msgForType(type), type, file, line, next);
  }

  this(string msg, string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    this(msg, Type.UNKNOWN, file, line, next);
  }

  this(string msg, Type type, string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    super(msg, file, line, next);
    type_ = type;
  }

  this(string msg, Type type, int errno, string file = __FILE__,
    size_t line = __LINE__, Throwable next = null)
  {
    this(msg ~ ": " ~ to!string(strerror(errno)), type, file, line, next);
  }

  Type type() const nothrow @property {
    return type_;
  }

protected:
  /** Error code */
  Type type_;
}
