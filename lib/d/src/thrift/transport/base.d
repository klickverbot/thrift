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
  bool isOpen() {
    return false;
  }

  /**
   * Tests whether there is more data to read or if the remote side is
   * still open. By default this is true whenever the transport is open,
   * but implementations should add logic to test for this condition where
   * possible (i.e. on a socket).
   * This is used by a server to check if it should listen for another
   * request.
   */
  bool peek() {
    return isOpen();
  }

  /**
   * Opens the transport for communications.
   *
   * Throws: TTransportException if opening failed.
   */
  void open() {
    throw new TTransportException(TTransportException.Type.NOT_OPEN,
      "Cannot open base TTransport.");
  }

  /**
   * Closes the transport.
   *
   * Throws: TTransportException if closing failed.
   */
  void close() {
    throw new TTransportException(TTransportException.Type.NOT_OPEN,
      "Cannot close base TTransport.");
  }

  /**
   * Attempt to read up to the specified number of bytes into the string.
   *
   * @param buf  Reference to the location to write the data
   * @return How many bytes were actually read
   * @throws TTransportException If an error occurs
   */
  uint read(ubyte[] buf) {
    throw new TTransportException(TTransportException.Type.NOT_OPEN,
      "Base TTransport cannot read.");
  }

  /**
   * Reads the given amount of data in its entirety no matter what.
   *
   * @param s     Reference to location for read data
   * @param len   How many bytes to read
   * @return How many bytes read, which must be equal to size
   * @throws TTransportException If insufficient data was read
   */
  void readAll(ubyte[] buf) {
    uint have = 0;
    uint get = 0;

    while (have < buf.length) {
      get = read(buf[have..$]);
      if (get <= 0) {
        throw new TTransportException(TTransportException.Type.END_OF_FILE,
          "No more data to read.");
      }
      have += get;
    }
  }

  /**
   * Called when read is completed.
   * This can be over-ridden to perform a transport-specific action
   * e.g. logging the request to a file
   *
   * @return number of bytes read if available, 0 otherwise.
   */
  uint readEnd() {
    // default behaviour is to do nothing
    return 0;
  }

  /**
   * Writes the string in its entirety to the buffer.
   *
   * Note: You must call flush() to ensure the data is actually written,
   * and available to be read back in the future.  Destroying a TTransport
   * object does not automatically flush pending data--if you destroy a
   * TTransport object with written but unflushed data, that data may be
   * discarded.
   *
   * @param buf  The data to write out
   * @throws TTransportException if an error occurs
   */
  void write(in ubyte[] buf) {
    throw new TTransportException(TTransportException.Type.NOT_OPEN,
                              "Base TTransport cannot write.");
  }

  /**
   * Called when write is completed.
   * This can be over-ridden to perform a transport-specific action
   * at the end of a request.
   *
   * @return number of bytes written if available, 0 otherwise
   */
  uint writeEnd() {
    // default behaviour is to do nothing
    return 0;
  }

  /**
   * Flushes any pending data to be written. Typically used with buffered
   * transport mechanisms.
   *
   * @throws TTransportException if an error occurs
   */
  void flush() {
    // default behaviour is to do nothing
  }

  /**
   * Attempts to return a pointer to \c len bytes, possibly copied into \c buf.
   * Does not consume the bytes read (i.e.: a later read will return the same
   * data).  This method is meant to support protocols that need to read
   * variable-length fields.  They can attempt to borrow the maximum amount of
   * data that they will need, then consume (see next method) what they
   * actually use.  Some transports will not support this method and others
   * will fail occasionally, so protocols must be prepared to use read if
   * borrow fails.
   *
   * @oaram buf  A buffer where the data can be stored if needed.
   *             If borrow doesn't return buf, then the contents of
   *             buf after the call are undefined.  This parameter may be
   *             NULL to indicate that the caller is not supplying storage,
   *             but would like a pointer into an internal buffer, if
   *             available.
   * @param len  *len should initially contain the number of bytes to borrow.
   *             If borrow succeeds, *len will contain the number of bytes
   *             available in the returned pointer.  This will be at least
   *             what was requested, but may be more if borrow returns
   *             a pointer to an internal buffer, rather than buf.
   *             If borrow fails, the contents of *len are undefined.
   * @return If the borrow succeeds, return a pointer to the borrowed data.
   *         This might be equal to \c buf, or it might be a pointer into
   *         the transport's internal buffers.
   * @throws TTransportException if an error occurs
   */
  const(ubyte)* borrow(ubyte* buf, ref uint len) {
    return null;
  }

  /**
   * Remove len bytes from the transport. This should always follow a borrow
   * of at least len bytes, and should always succeed.
   * TODO(dreiss): Is there any transport that could borrow but fail to
   * consume, or that would require a buffer to dump the consumed data?
   *
   * @param len  How many bytes to consume
   * @throws TTransportException If an error occurs
   */
  void consume(uint len) {
    throw new TTransportException(TTransportException.Type.NOT_OPEN,
      "Base TTransport cannot consume.");
  }

protected:
  this() {}
};

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
