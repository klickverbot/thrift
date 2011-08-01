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
import std.conv : text;
import thrift.base;

/**
 * Generic interface for a method of transporting data. A TTransport may be
 * capable of either reading or writing, but not necessarily both.
 */
interface TTransport {
  /**
   * Whether this transport is open.
   */
  bool isOpen() @property;

  /**
   * Tests whether there is more data to read or if the remote side is
   * still open.
   *
   * This is used by a server to check if it should listen for another request.
   */
  bool peek();

  /**
   * Opens the transport for communications.
   *
   * If the transport is already open, nothing happens.
   *
   * Throws: TTransportException if opening fails.
   */
  void open();

  /**
   * Closes the transport.
   *
   * If the transport is not open, nothing happens.
   *
   * Throws: TTransportException if closing fails.
   */
  void close();

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
    version(none) assert(isOpen, "Called read() on non-open transport!");
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
    // DMD @@BUG6108@@: Uncommenting the assertion leads to crashing
    // thrift.transport.framed unittests.
    version(none) assert(isOpen, "Called readAll() on non-open transport!");
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
    // DMD @@BUG6108@@: Uncommenting the assertion leads to crashing
    // ThriftTest SSL server.
    version(none) assert(isOpen, "Called readEnd() on non-open transport!");
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
    // DMD @@BUG6108@@: Uncommenting the assertion leads to crashing
    // thrift.transport.memory unittests.
    version(none) assert(isOpen, "Called write() on non-open transport!");
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
    // DMD @@BUG6108@@: Uncommenting the assertion leads to crashing
    // ThriftTest SSL server.
    version(none) assert(isOpen, "Called writeEnd() on non-open transport!");
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
    // DMD @@BUG6108@@: Uncommenting the assertion leads to crashing
    // thrift.transport.framed unittests.
    version(none) assert(isOpen, "Called flush() on non-open transport!");
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
    // DMD @@BUG6108@@: Uncommenting the assertion leads to crashing
    // thrift.transport.memory unittests.
    version(none) assert(isOpen, "Called borrow() on non-open transport!");
  } out (result) {
    // FIXME: Commented out because len gets corrupted in
    // thrift.transport.memory borrow() unittest.
    version(none) assert(result is null || result.length >= len,
       "Buffer returned by borrow() too short.");
  }

  /**
   * Remove len bytes from the transport. This must always follow a borrow
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
    // DMD @@BUG6108@@: Uncommenting the assertion leads to crashing
    // thrift.transport.memory unittests.
    version(none) assert(isOpen, "Called consume() on non-open transport!");
  }
}

/**
 * Provides basic fall-back implementations of the TTransport interface.
 */
class TBaseTransport : TTransport {
  override bool isOpen() @property {
    return false;
  }

  override bool peek() {
    return isOpen;
  }

  override void open() {
    throw new TTransportException("Cannot open TBaseTransport.",
      TTransportException.Type.NOT_IMPLEMENTED);
  }

  override void close() {
    throw new TTransportException("Cannot close TBaseTransport.",
      TTransportException.Type.NOT_IMPLEMENTED);
  }

  override size_t read(ubyte[] buf) {
    throw new TTransportException("Cannot read from a TBaseTransport.",
      TTransportException.Type.NOT_IMPLEMENTED);
  }

  override void readAll(ubyte[] buf) {
    size_t have;
    while (have < buf.length) {
      size_t get = read(buf[have..$]);
      if (get <= 0) {
        throw new TTransportException(text("Could not readAll() ", buf.length,
          " bytes as no more data was available after ", have, " bytes."),
          TTransportException.Type.END_OF_FILE);
      }
      have += get;
    }
  }

  override size_t readEnd() {
    // Do nothing by default, not needed by all implementations.
    return 0;
  }

  override void write(in ubyte[] buf) {
    throw new TTransportException("Cannot write to a TBaseTransport.",
      TTransportException.Type.NOT_IMPLEMENTED);
  }

  override size_t writeEnd() {
    // Do nothing by default, not needed by all implementations.
    return 0;
  }

  override void flush() {
    // Do nothing by default, not needed by all implementations.
  }

  override const(ubyte)[] borrow(ubyte* buf, size_t len) {
    // borrow() is allowed to fail anyway, so just return null.
    return null;
  }

  override void consume(size_t len) {
    throw new TTransportException("Cannot consume from a TBaseTransport.",
      TTransportException.Type.NOT_IMPLEMENTED);
  }

protected:
  this() {}
}

/**
 * Makes a TTransport given a source transport. This is commonly used inside
 * server implementations to wrap the raw client connections into e.g. a
 * buffered or compressed transport.
 */
class TTransportFactory {
  /**
   * Default implementation does nothing, just returns the transport given.
   */
  TTransport getTransport(TTransport trans) {
    return trans;
  }
}

/**
 * Transport factory for transports which simply wrap an underlying TTransport
 * without requiring additional configuration.
 */
template TWrapperTransportFactory(T) if (
  is(T : TTransport) && __traits(compiles, new T(TTransport.init))
) {
  class TWrapperTransportFactory : TTransportFactory {
    override T getTransport(TTransport trans) {
      return new T(trans);
    }
  }
}

/**
 * Transport-level exception.
 */
class TTransportException : TException {
  /**
   * Error codes for the various types of exceptions.
   */
  enum Type {
    UNKNOWN, ///
    NOT_OPEN, ///
    TIMED_OUT, ///
    END_OF_FILE, ///
    INTERRUPTED, ///
    BAD_ARGS, ///
    CORRUPTED_DATA, ///
    INTERNAL_ERROR, ///
    NOT_IMPLEMENTED ///
  }

  ///
  this(Type type, string file = __FILE__, size_t line = __LINE__, Throwable next = null) {
    string msg = "TTransportException: ";
    switch (type) {
      case Type.UNKNOWN: msg ~= "Unknown transport exception"; break;
      case Type.NOT_OPEN: msg ~= "Transport not open"; break;
      case Type.TIMED_OUT: msg ~= "Timed out"; break;
      case Type.END_OF_FILE: msg ~= "End of file"; break;
      case Type.INTERRUPTED: msg ~= "Interrupted"; break;
      case Type.BAD_ARGS: msg ~= "Invalid arguments"; break;
      case Type.CORRUPTED_DATA: msg ~= "Corrupted Data"; break;
      case Type.INTERNAL_ERROR: msg ~= "Internal error"; break;
      case Type.NOT_IMPLEMENTED: msg ~= "Not implemented"; break;
      default: msg ~= "(Invalid exception type)"; break;
    }

    this(msg, type, file, line, next);
  }

  ///
  this(string msg, string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    this(msg, Type.UNKNOWN, file, line, next);
  }

  ///
  this(string msg, Type type, string file = __FILE__, size_t line = __LINE__,
    Throwable next = null)
  {
    super(msg, file, line, next);
    type_ = type;
  }

  ///
  this(string msg, Type type, int errno, string file = __FILE__,
    size_t line = __LINE__, Throwable next = null)
  {
    this(text(": ", strerror(errno)), type, file, line, next);
  }

  ///
  Type type() const nothrow @property {
    return type_;
  }

protected:
  Type type_;
}

/**
 * Metaprogramming helper returning whether the passed type is a TTransport
 * implementation.
 */
template isTTransport(T) {
  enum isTTransport = is(T : TTransport);
}
