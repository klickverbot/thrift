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
 * Crude port of the C++ buffered transport.
 *
 * TODO: Implement this in ideomatic D.
 */
module thrift.transport.buffered;

import std.algorithm : min;
import std.exception : enforce;
import thrift.transport.base;

class TBufferBase : TBaseTransport {
  /**
   * Fast-path read.
   *
   * When we have enough data buffered to fulfill the read, we can satisfy it
   * with a single memcpy, then adjust our internal pointers.  If the buffer
   * is empty, we call out to our slow path, implemented by a subclass.
   * This method is meant to eventually be nonvirtual and inlinable.
   */
  override size_t read(ubyte[] buf) {
    ubyte* new_rBase = rBase_ + buf.length;
    if (new_rBase <= rBound_) {
      buf[] = rBase_[0..buf.length];
      rBase_ = new_rBase;
      return buf.length;
    }
    return readSlow(buf);
  }

  /**
   * Shortcutted version of readAll.
   */
  override void readAll(ubyte[] buf) {
    ubyte* new_rBase = rBase_ + buf.length;
    if (new_rBase <= rBound_) {
      buf[] = rBase_[0..buf.length];
      rBase_ = new_rBase;
      return;
    }
    super.readAll(buf);
  }

  /**
   * Fast-path write.
   *
   * When we have enough empty space in our buffer to accomodate the write, we
   * can satisfy it with a single memcpy, then adjust our internal pointers.
   * If the buffer is full, we call out to our slow path, implemented by a
   * subclass.  This method is meant to eventually be nonvirtual and
   * inlinable.
   */
  override void write(in ubyte[] buf) {
    ubyte* new_wBase = wBase_ + buf.length;
    if (new_wBase <= wBound_) {
      wBase_[0..buf.length] = buf;
      wBase_ = new_wBase;
      return;
    }
    writeSlow(buf);
  }

  /**
   * Fast-path borrow.  A lot like the fast-path read.
   */
  override const(ubyte)[] borrow(ubyte* buf, size_t len) {
    if (cast(ptrdiff_t)len <= rBound_ - rBase_) {
      return rBase_[0 .. (rBound_ - rBase_)];
    }
    return borrowSlow(buf, len);
  }

  /**
   * Consume doesn't require a slow path.
   */
  override void consume(size_t len) {
    enforce(cast(ptrdiff_t)len <= rBound_ - rBase_, new TTransportException(
      TTransportException.Type.BAD_ARGS, "consume did not follow a borrow."));
    rBase_ += len;
  }


protected:
  /// Slow path read.
  abstract size_t readSlow(ubyte[] buf);

  /// Slow path write.
  abstract void writeSlow(in ubyte[] buf);

  /// Slow path borrow.
  abstract const(ubyte)[] borrowSlow(ubyte* buf, size_t len);

  /**
   * Trivial constructor.
   *
   * Initialize pointers safely.  Constructing is not a very
   * performance-sensitive operation, so it is okay to just leave it to
   * the concrete class to set up pointers correctly.
   */
  this() {}

  /// Convenience mutator for setting the read buffer.
  void setReadBuffer(ubyte[] buf) {
    rBase_ = buf.ptr;
    rBound_ = buf.ptr + buf.length;
  }

  /// Convenience mutator for setting the write buffer.
  void setWriteBuffer(ubyte[] buf) {
    wBase_ = buf.ptr;
    wBound_ = buf.ptr + buf.length;
  }

  /// Reads begin here.
  ubyte* rBase_;
  /// Reads may extend to just before here.
  ubyte* rBound_;

  /// Writes begin here.
  ubyte* wBase_;
  /// Writes may extend to just before here.
  ubyte* wBound_;
}

final class TBufferedTransport : TBufferBase {
  enum int DEFAULT_BUFFER_SIZE = 512;

  /// Use default buffer sizes.
  this(TTransport transport) {
    transport_ = transport;
    rBuf_ = new ubyte[DEFAULT_BUFFER_SIZE];
    wBuf_ = new ubyte[DEFAULT_BUFFER_SIZE];
    initPointers();
  }

  /// Use specified buffer sizes.
  this(TTransport transport, uint sz) {
    transport_ = transport;
    rBuf_ = new ubyte[sz];
    wBuf_ = new ubyte[sz];
    initPointers();
  }

  /// Use specified read and write buffer sizes.
  this(TTransport transport, uint rsz, uint wsz) {
    transport_ = transport;
    rBuf_ = new ubyte[rsz];
    wBuf_ = new ubyte[wsz];
    initPointers();
  }

  override void open() {
    transport_.open();
  }

  override bool isOpen() {
    return transport_.isOpen();
  }

  override bool peek() {
    if (rBase_ == rBound_) {
      setReadBuffer(rBuf_[0..transport_.read(rBuf_)]);
    }
    return (rBound_ > rBase_);
  }

  override void flush() {
    // Write out any data waiting in the write buffer.
    auto have_bytes = wBase_ - wBuf_.ptr;
    if (have_bytes > 0) {
      // Note that we reset wBase_ prior to the underlying write
      // to ensure we're in a sane state (i.e. internal buffer cleaned)
      // if the underlying write throws up an exception
      wBase_ = wBuf_.ptr;
      transport_.write(wBuf_[0..have_bytes]);
    }

    // Flush the underlying transport.
    transport_.flush();
  }

  override void close() {
    flush();
    transport_.close();
  }

  TTransport underlyingTransport() @property {
    return transport_;
  }

protected:
  override size_t readSlow(ubyte[] buf) {
    size_t have = rBound_ - rBase_;

    // We should only take the slow path if we can't satisfy the read
    // with the data already in the buffer.
    assert(have < buf.length);

    // If we have some date in the buffer, copy it out and return it.
    // We have to return it without attempting to read more, since we aren't
    // guaranteed that the underlying transport actually has more data, so
    // attempting to read from it could block.
    if (have > 0) {
      buf[0..have] = rBase_[0..have];
      setReadBuffer(rBuf_[0..0]);
      return have;
    }

    // No data is available in our buffer.
    // Get more from underlying transport up to buffer size.
    // Note that this makes a lot of sense if len < rBufSize_
    // and almost no sense otherwise.  TODO(dreiss): Fix that
    // case (possibly including some readv hotness).
    setReadBuffer(rBuf_[0..transport_.read(rBuf_)]);

    // Hand over whatever we have.
    auto give = min(buf.length, cast(size_t)(rBound_ - rBase_));
    buf[0..give] = rBase_[0..give];
    rBase_ += give;

    return give;
  }

  override void writeSlow(in ubyte[] buf) {
    auto have_bytes = wBase_ - wBuf_.ptr;
    auto space = wBound_ - wBase_;
    // We should only take the slow path if we can't accomodate the write
    // with the free space already in the buffer.
    assert(wBound_ - wBase_ < cast(ptrdiff_t)(buf.length));

    // Now here's the tricky question: should we copy data from buf into our
    // internal buffer and write it from there, or should we just write out
    // the current internal buffer in one syscall and write out buf in another.
    // If our currently buffered data plus buf is at least double our buffer
    // size, we will have to do two syscalls no matter what (except in the
    // degenerate case when our buffer is empty), so there is no use copying.
    // Otherwise, there is sort of a sliding scale.  If we have N-1 bytes
    // buffered and need to write 2, it would be crazy to do two syscalls.
    // On the other hand, if we have 2 bytes buffered and are writing 2N-3,
    // we can save a syscall in the short term by loading up our buffer, writing
    // it out, and copying the rest of the bytes into our buffer.  Of course,
    // if we get another 2-byte write, we haven't saved any syscalls at all,
    // and have just copied nearly 2N bytes for nothing.  Finding a perfect
    // policy would require predicting the size of future writes, so we're just
    // going to always eschew syscalls if we have less than 2N bytes to write.

    // The case where we have to do two syscalls.
    // This case also covers the case where the buffer is empty,
    // but it is clearer (I think) to think of it as two separate cases.
    if ((have_bytes + buf.length >= 2*wBuf_.length) || (have_bytes == 0)) {
      // TODO(dreiss): writev
      if (have_bytes > 0) {
        transport_.write(wBuf_.ptr[0..have_bytes]);
      }
      transport_.write(buf);
      wBase_ = wBuf_.ptr;
      return;
    }

    // Fill up our internal buffer for a write.
    wBase_[0..space] = buf[0..space];
    auto newBuf = buf[space..$];
    transport_.write(wBuf_);

    // Copy the rest into our buffer.
    wBuf_[0..newBuf.length] = newBuf[];
    wBase_ = wBuf_.ptr + newBuf.length;
  }

  /**
   * The following behavior is currently implemented by TBufferedTransport,
   * but that may change in a future version:
   * 1/ If len is at most rBufSize_, borrow will never return NULL.
   *    Depending on the underlying transport, it could throw an exception
   *    or hang forever.
   * 2/ Some borrow requests may copy bytes internally.  However,
   *    if len is at most rBufSize_/2, none of the copied bytes
   *    will ever have to be copied again.  For optimial performance,
   *    stay under this limit.
   */
  override const(ubyte)[] borrowSlow(ubyte* buf, size_t len) {
    return null;
  }

  void initPointers() {
    setReadBuffer(rBuf_[0..0]);
    setWriteBuffer(wBuf_);
    // Write size never changes.
  }

  TTransport transport_;

  ubyte[] rBuf_;
  ubyte[] wBuf_;
}

/**
 * Wraps passed in transports in a TBufferedTransports.
 */
alias TWrapperTransportFactory!TBufferedTransport TBufferedTransportFactory;
