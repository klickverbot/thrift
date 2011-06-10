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
module thrift.transport.file;

import core.thread : Thread;
import std.array : empty;
import std.algorithm : min;
import std.concurrency;
import std.conv : to;
import std.datetime : AutoStart, dur, Clock, Duration, StopWatch;
import std.exception;
import std.stdio : stderr, File; // No proper error logging yet.
import thrift.transport.base;

/// The default chunk size, in bytes.
enum DEFAULT_CHUNK_SIZE = 16 * 1024 * 1024;

/**
 * A transport used to write files. It can never be read from, calling read()
 * throws.
 *
 * Contrary to the C++ design, explicitly opening the transport/file before
 * using is necessary to allow manually closing the file without relying on the
 * object lifetime. Otherwise, it's a straight port of the C++ implementation.
 */
class TFileReaderTransport : TBaseTransport {
  /**
   * Creates a new file writer transport.
   *
   * Params:
   *   path = Path of the file to opperate on.
   */
  this(string path) {
    path_ = path;
    chunkSize_ = DEFAULT_CHUNK_SIZE;
    readBufferSize_ = DEFAULT_READ_BUFFER_SIZE;
    readTimeout_ = DEFAULT_READ_TIMEOUT;
    corruptedEventSleepDuration_ = DEFAULT_CORRUPTED_EVENT_SLEEP_DURATION;
    maxEventSize = DEFAULT_MAX_EVENT_SIZE;
  }

  override bool isOpen() @property {
    return isOpen_;
  }

  override bool peek() {
    if (!isOpen) return false;

    // If there is no event currently processed, try fetching one from the
    // file.
    if (!currentEvent_) {
      currentEvent_ = readEvent();

      if (!currentEvent_) {
        // Still nothing there, couldn't read a new event.
        return false;
      }
    }
    // check if there is anything to read
    return (currentEvent_.length - currentEventPos_) > 0;
  }

  override void open() {
    if (isOpen) return;
    try {
      file_ = File(path_, "rb");
    } catch (Exception e) {
      throw new TTransportException("Error on opening input file.",
        TTransportException.Type.NOT_OPEN, __FILE__, __LINE__, e);
    }
    isOpen_ = true;
  }

  override void close() {
    file_.close();
    isOpen_ = false;
    readState_.resetAllValues();
  }

  override size_t read(ubyte[] buf) {
    // If there is no event currently processed, try fetching one from the
    // file.
    if (!currentEvent_) {
      currentEvent_ = readEvent();

      if (!currentEvent_) {
        // Still nothing there, couldn't read a new event.
        return 0;
      }
    }

    // read as much of the current event as possible
    auto len = buf.length;
    auto remaining = currentEvent_.length - currentEventPos_;
    if (remaining <= len) {
      buf[0 .. remaining] = currentEvent_[currentEventPos_ .. $];
      currentEvent_ = null;
      return remaining;
    }

    // read as much as possible
    buf[] = currentEvent_[currentEventPos_ .. currentEventPos_ + len];
    currentEventPos_ += len;
    return len;
  }

  ulong getNumChunks() {
    enforce(isOpen, new TTransportException(
      "Cannot get number of chunks if file not open.",
      TTransportException.Type.NOT_OPEN));

    try {
      auto fileSize = file_.size();
      if (fileSize == 0) {
        // Empty files have no chunks.
        return 0;
      }
      return ((fileSize)/chunkSize_) + 1;
    } catch (Exception e) {
      throw new TTransportException("Error getting file size.", __FILE__,
        __LINE__, e);
    }
  }

  ulong getCurChunk() {
    return offset_ / chunkSize_;
  }

  void seekToChunk(long chunk) {
    enforce(isOpen, new TTransportException(
      "Cannot get number of chunks if file not open.",
      TTransportException.Type.NOT_OPEN));

    auto numChunks = getNumChunks();

    if (chunk < 0) {
      // Count negative indices from the end.
      chunk += numChunks;
    }

    if (chunk < 0) {
      stderr.writefln("Incorrect chunk number for reverse seek, seeking to " ~
       "beginning instead: %s", chunk);
      chunk = 0;
    }

    bool seekToEnd;
    long minEndOffset;
    if (chunk >= numChunks) {
      stderr.writefln("Trying to seek to non-existing chunk, seeking to " ~
       "end of file instead: %s", chunk);
      seekToEnd = true;
      chunk = numChunks - 1;
      // this is the min offset to process events till
      minEndOffset = file_.size();
    }

    readState_.resetAllValues();
    currentEvent_ = null;

    try {
      file_.seek(chunk * chunkSize_);
      offset_ = chunk * chunkSize_;
    } catch (Exception e) {
      throw new TTransportException("Error seeking to chunk", __FILE__,
        __LINE__, e);
    }

    if (seekToEnd) {
      // Never wait on the end of the file for new content, we just want to
      // find the last one.
      auto oldReadTimeout = readTimeout_;
      scope (exit) readTimeout_ = oldReadTimeout;
      readTimeout_ = dur!"hnsecs"(0);

      // Keep on reading unti the last event at point of seekToChunk call.
      while ((offset_ + readState_.bufferPos_) < minEndOffset) {
        if (readEvent() is null) {
          break;
        }
      }
    }
  }

  void seekToEnd() {
    seekToChunk(getNumChunks());
  }

  /**
   * The size of the chunks the file is divided into, in bytes.
   */
  size_t chunkSize() @property {
    return chunkSize_;
  }

  /// ditto
  void chunkSize(size_t value) @property {
    enforce(!isOpen, new TTransportException(
      "Cannot set chunk size after TFileReaderTransport has been opened."));
    chunkSize_ = value;
  }

  /**
   * If positive, wait the specified duration for new data when arriving at
   * end of file. If negative, wait forever (tailing mode), waking up to check
   * in the specified interval. If zero, do not wait at all.
   *
   * Defaults to 500 ms.
   */
  Duration readTimeout() @property {
    return readTimeout_;
  }

  /// ditto
  void readTimeout(Duration value) @property {
    readTimeout_ = value;
  }

  /// ditto
  enum DEFAULT_READ_TIMEOUT = dur!"msecs"(500);

  /*
   * Read buffer size, in bytes.
   *
   * Defaults to 1 MiB.
   */
  size_t readBufferSize() @property {
    return readBufferSize_;
  }

  /// ditto
  void readBufferSize(size_t value) @property {
    if (readBuffer_) {
      enforce(value <= readBufferSize_,
        "Cannot shrink read buffer after first read.");
      readBuffer_.length = value;
    }
    readBufferSize_ = value;
  }

  /// ditto
  enum DEFAULT_READ_BUFFER_SIZE = 1 * 1024 * 1024;

  /**
   * Arbitrary event size limit, in bytes. Must be smaller than chunk size.
   *
   * Defaults to zero (no limit).
   */
  size_t maxEventSize() @property {
    return maxEventSize_;
  }

  /// ditto
  void maxEventSize(size_t value) @property {
    enforce(value <= chunkSize_ - uint.sizeof, "Events cannot span " ~
      "mutiple chunks, maxEventSize must be smaller than chunk size.");
    maxEventSize_ = value;
  }

  /// ditto
  enum DEFAULT_MAX_EVENT_SIZE = 0;

  /**
   * The interval at which the thread wakes up to check for the next chunk
   * in tailing mode.
   *
   * Defaults to one second.
   */
  Duration corruptedEventSleepDuration() {
    return corruptedEventSleepDuration_;
  }

  /// ditto
  void corruptedEventSleepDuration(Duration value) {
    corruptedEventSleepDuration_ = value;
  }

  /// ditto
  enum DEFAULT_CORRUPTED_EVENT_SLEEP_DURATION = dur!"seconds"(1);

  /**
   * The maximum number of corrupted events tolerated before the whole chunk
   * is skipped.
   *
   * Defaults to zero.
   */
  uint maxCorruptedEvents() @property {
    return maxCorruptedEvents_;
  }

  /// ditto
  void maxCorruptedEvents(uint value) @property {
    maxCorruptedEvents_ = value;
  }

  /// ditto
  enum DEFAULT_MAX_CORRUPTED_EVENTS = 0;

private:
  ubyte[] readEvent() {
    if (!readBuffer_) {
      readBuffer_ = new ubyte[readBufferSize_];
    }

    bool timeoutExpired;
    while (1) {
      // read from the file if read buffer is exhausted
      if (readState_.bufferPos_ == readState_.bufferLen_) {
        // advance the offset pointer
        offset_ += readState_.bufferLen_;

        try {
          auto usedBuf = file_.rawRead(readBuffer_);
          readState_.bufferLen_ = usedBuf.length;
        } catch (Exception e) {
          readState_.resetAllValues();
          throw new TTransportException("Error while reading from file",
            __FILE__, __LINE__, e);
        }

        readState_.bufferPos_ = 0;
        readState_.lastDispatchPos_ = 0;

        if (readState_.bufferLen_ == 0) {
          // Reached end of file.
          if (readTimeout_ < dur!"hnsecs"(0)) {
            // Tailing mode, sleep for the specified duration and try again.
            Thread.sleep(-readTimeout_);
            continue;
          } else if (readTimeout_ == dur!"hnsecs"(0) || timeoutExpired) {
            // Either no timeout set, or it has already expired.
            readState_.resetState(0);
            return null;
          } else {
            // Timeout mode, sleep for the specified amount of time and retry.
            Thread.sleep(readTimeout_);
            timeoutExpired = true;
            continue;
          }
        }
      }

      // Attempt to read an event from the buffer.
      while (readState_.bufferPos_ < readState_.bufferLen_) {
        if (readState_.readingSize_) {
          if (readState_.eventSizeBuffPos_ == 0) {
            if ((offset_ + readState_.bufferPos_)/chunkSize_ !=
              ((offset_ + readState_.bufferPos_ + 3)/chunkSize_))
            {
              readState_.bufferPos_++;
              continue;
            }
          }

          readState_.eventSizeBuff_[readState_.eventSizeBuffPos_++] =
            readBuffer_[readState_.bufferPos_++];

          if (readState_.eventSizeBuffPos_ == 4) {
            auto size = (cast(uint[])readState_.eventSizeBuff_)[0];

            if (size == 0) {
              // This is part of the zero padding between chunks.
              readState_.resetState(readState_.lastDispatchPos_);
              continue;
            }

            // got a valid event
            readState_.readingSize_ = false;
            readState_.eventLen_ = size;
            readState_.eventPos_ = 0;

            // check if the event is corrupted and perform recovery if required
            if (isEventCorrupted()) {
              performRecovery();
              // start from the top
              break;
            }
          }
        } else {
          if (!readState_.event_) {
            readState_.event_ = new ubyte[readState_.eventLen_];
          }

          // take either the entire event or the remaining bytes in the buffer
          auto reclaimBuffer = min(readState_.bufferLen_ - readState_.bufferPos_,
            readState_.eventLen_ - readState_.eventPos_);

          // copy data from read buffer into event buffer
          readState_.event_[
            readState_.eventPos_ .. readState_.eventPos_ + reclaimBuffer
          ] = readBuffer_[
            readState_.bufferPos_ .. readState_.bufferPos_ + reclaimBuffer
          ];

          // increment position ptrs
          readState_.eventPos_ += reclaimBuffer;
          readState_.bufferPos_ += reclaimBuffer;

          // check if the event has been read in full
          if (readState_.eventPos_ == readState_.eventLen_) {
            // Reset the read state and return the completed event.
            auto completeEvent = readState_.event_;
            readState_.event_ = null;
            readState_.resetState(readState_.bufferPos_);
            return completeEvent;
          }
        }
      }
    }
  }

  bool isEventCorrupted() {
    // an error is triggered if:
    if ((maxEventSize_ > 0) && (readState_.eventLen_ > maxEventSize_)) {
      // 1. Event size is larger than user-speficied max-event size
      stderr.writefln("Read corrupt event. Event size(%s) greater than max " ~
        "event size (%s)", readState_.eventLen_, maxEventSize_);
      return true;
    } else if (readState_.eventLen_ > chunkSize_) {
      // 2. Event size is larger than chunk size
      stderr.writefln("Read corrupt event. Event size(%s) greater than " ~
        "chunk size (%s)", readState_.eventLen_, chunkSize_);
      return true;
    } else if (((offset_ + readState_.bufferPos_ - uint.sizeof) / chunkSize_) !=
      ((offset_ + readState_.bufferPos_ + readState_.eventLen_ -
      uint.sizeof) / chunkSize_))
    {
      // 3. size indicates that event crosses chunk boundary
      stderr.writefln("Read corrupt event. Event crosses chunk boundary. " ~
        "Event size: %s. Offset: %s",
        readState_.eventLen_,
        (offset_ + readState_.bufferPos_ + uint.sizeof)
      );

      return true;
    }

    return false;
  }

  void performRecovery() {
    // perform some kickass recovery
    auto curChunk = getCurChunk();
    if (lastBadChunk_ == curChunk) {
      numCorruptedEventsInChunk_++;
    } else {
      lastBadChunk_ = curChunk;
      numCorruptedEventsInChunk_ = 1;
    }

    if (numCorruptedEventsInChunk_ < maxCorruptedEvents_) {
      // maybe there was an error in reading the file from disk
      // seek to the beginning of chunk and try again
      seekToChunk(curChunk);
    } else {
      // Just skip ahead to the next chunk if we not already at the last chunk.
      if (curChunk != (getNumChunks() - 1)) {
        seekToChunk(curChunk + 1);
      } else if (readTimeout_ < dur!"hnsecs"(0)) {
        // We are in tailing mode, wait until there is enough data to start
        // the next chunk.
        while(curChunk == (getNumChunks() - 1)) {
          Thread.sleep(corruptedEventSleepDuration_);
        }
        seekToChunk(curChunk + 1);
      } else {
        // Pretty hosed at this stage, rewind the file back to the last
        // successful point and punt on the error.
        readState_.resetState(readState_.lastDispatchPos_);
        currentEvent_ = null;

        throw new TTransportException("File corrupted at offset: " ~
          to!string(offset_ + readState_.lastDispatchPos_),
          TTransportException.Type.CORRUPTED_DATA);
      }
    }
  }

  string path_;
  File file_;
  bool isOpen_;
  long offset_;
  ubyte[] currentEvent_;
  size_t currentEventPos_;
  size_t chunkSize_;
  Duration readTimeout_;
  size_t maxEventSize_;

  // Read buffer – lazily allocated on the first read().
  ubyte[] readBuffer_;
  size_t readBufferSize_;

  static struct ReadState {
    ubyte[] event_;
    size_t eventLen_;
    size_t eventPos_;

    // keep track of event size
    ubyte[4] eventSizeBuff_;
    ubyte eventSizeBuffPos_;
    bool readingSize_ = true;

    // read buffer variables
    size_t bufferPos_;
    size_t bufferLen_;

    // last successful dispatch point
    size_t lastDispatchPos_;

    void resetState(size_t lastDispatchPos) {
      readingSize_ = true;
      eventSizeBuffPos_ = 0;
      lastDispatchPos_ = lastDispatchPos;
    }

    void resetAllValues() {
      resetState(0);
      bufferPos_ = 0;
      bufferLen_ = 0;
      event_ = null;
    }
  }
  ReadState readState_;

  ulong lastBadChunk_;
  uint maxCorruptedEvents_;
  uint numCorruptedEventsInChunk_;
  Duration corruptedEventSleepDuration_;
}

/**
 * A transport used to write files. It can never be read from, calling read()
 * throws.
 *
 * Contrary to the C++ design, explicitly opening the transport/file before
 * using is necessary to allow manually closing the file without relying on the
 * object lifetime.
 */
class TFileWriterTransport : TBaseTransport {
  /**
   * Creates a new file writer transport.
   *
   * Params:
   *   path = Path of the file to opperate on.
   */
  this(string path) {
    path_ = path;

    chunkSize_ = DEFAULT_CHUNK_SIZE;
    eventBufferSize_ = DEFAULT_EVENT_BUFFER_SIZE;
    ioErrorSleepDuration = DEFAULT_IO_ERROR_SLEEP_DURATION;
    maxFlushBytes_ = DEFAULT_MAX_FLUSH_BYTES;
    maxFlushInterval_ = DEFAULT_MAX_FLUSH_INTERVAL;
  }

  override bool isOpen() @property {
    return isOpen_;
  }

  /**
   * A file writer transport can never be read from.
   */
  override bool peek() {
    return false;
  }

  override void open() {
    if (isOpen) return;

    writerThread_ = spawn(
      &writerThread,
      thisTid(),
      path_,
      chunkSize_,
      maxFlushBytes_,
      maxFlushInterval_,
      ioErrorSleepDuration_
    );
    setMaxMailboxSize(writerThread_, eventBufferSize_, OnCrowding.block);
    isOpen_ = true;
  }

  /**
   * Closes the transport, i.e. the underlying file and the writer thread.
   */
  override void close() {
    prioritySend(writerThread_, ShutdownMessage()); // FIXME: Should use normal send here.
    receive((ShutdownMessage msg, Tid tid){});
    isOpen_ = false;
  }

  /**
   * Enqueues the passed slice of data for writing and immediately returns.
   * write() only blocks if the event buffer has been exhausted.
   *
   * The transport must be open when calling this.
   *
   * Params:
   *   buf = Slice of data to write.
   */
  override void write(in ubyte[] buf) {
    enforce(isOpen, new TTransportException(
      "Cannot write to non-open transport.", TTransportException.Type.NOT_OPEN));
    if (buf.empty) {
      stderr.writeln("TFileWriterTransport: Cannot write empty event, skipping.");
      return;
    }

    auto maxSize = chunkSize - uint.sizeof;
    enforce(buf.length <= maxSize, new TTransportException(
      "Cannot write more than " ~ to!string(maxSize) ~
      "bytes at once due to chunk size."));

    send(writerThread_, buf.idup);
  }

  /**
   * Flushes any pending data to be written.
   *
   * The transport must be open when calling this.
   *
   * Throws: TTransportException if an error occurs.
   */
  override void flush() {
    send(writerThread_, FlushMessage());
    receive((FlushMessage msg, Tid tid){});
  }

  /**
   * The size of the chunks the file is divided into, in bytes.
   *
   * A single event (write call) never spans multiple chunks – this
   * effectively limits the event size to chunkSize - uint.sizeof.
   */
  size_t chunkSize() @property {
    return chunkSize_;
  }

  /// ditto
  void chunkSize(size_t value) @property {
    enforce(!isOpen, new TTransportException(
      "Cannot set chunk size after TFileWriterTransport has been opened."));
    chunkSize_ = value;
  }

  /**
   * The maximum number of write() calls buffered, or zero for no limit.
   *
   * If the buffer is exhausted, write() will block until space becomes
   * available.
   */
  size_t eventBufferSize() @property {
    return eventBufferSize_;
  }

  /// ditto
  void eventBufferSize(size_t value) @property {
    eventBufferSize_ = value;
    if (isOpen) {
      setMaxMailboxSize(writerThread_, value, OnCrowding.throwException);
    }
  }

  /// ditto
  enum DEFAULT_EVENT_BUFFER_SIZE = 10_000;

  /**
   * Maximum number of bytes buffered before writing and flushing the file
   * to disk.
   *
   * Currently cannot be set after the first call to write().
   */
  size_t maxFlushBytes() @property {
    return maxFlushBytes_;
  }

  /// ditto
  void maxFlushBytes(size_t value) @property {
    maxFlushBytes_ = value;
    if (isOpen) {
      send(writerThread_, FlushBytesMessage(value));
    }
  }

  /// ditto
  enum DEFAULT_MAX_FLUSH_BYTES = 1000 * 1024;

  /**
   * Maximum interval between flushing the file to disk.
   *
   * Currenlty cannot be set after the first call to write().
   */
  Duration maxFlushInterval() @property {
    return maxFlushInterval_;
  }

  /// ditto
  void maxFlushInterval(Duration value) @property {
    maxFlushInterval_ = value;
    if (isOpen) {
      send(writerThread_, FlushIntervalMessage(value));
    }
  }

  /// ditto
  enum DEFAULT_MAX_FLUSH_INTERVAL = dur!"seconds"(3);

  /**
   * When the writer thread encounteres an I/O error, it goes pauses for a
   * short time before trying to reopen the output file. This controls the
   * sleep duration.
   */
  Duration ioErrorSleepDuration() @property {
    return ioErrorSleepDuration_;
  }

  /// ditto
  void ioErrorSleepDuration(Duration value) @property {
    ioErrorSleepDuration_ = value;
    if (isOpen) {
      send(writerThread_, FlushIntervalMessage(value));
    }
  }

  /// ditto
  enum DEFAULT_IO_ERROR_SLEEP_DURATION = dur!"msecs"(500);

private:
  string path_;
  size_t chunkSize_;
  size_t eventBufferSize_;
  Duration ioErrorSleepDuration_;
  size_t maxFlushBytes_;
  Duration maxFlushInterval_;
  bool isOpen_;
  Tid writerThread_;
}

private {
  // Signals that the file should be flushed on disk. Sent to the writer
  // thread and sent back along with the tid for confirmation.
  struct FlushMessage {}

  // Signals that the writer thread should close the file and shut down. Sent
  // to the writer thread and sent back along with the tid for confirmation.
  struct ShutdownMessage {}

  struct FlushBytesMessage {
    size_t value;
  }

  struct FlushIntervalMessage {
    Duration value;
  }

  struct IoErrorSleepDurationMessage {
    Duration value;
  }

  void writerThread(
    Tid owner,
    string path,
    size_t chunkSize,
    size_t maxFlushBytes,
    Duration maxFlushInterval,
    Duration ioErrorSleepDuration
  ) {
    bool hasIOError;
    File file;
    try {
      file = File(path, "ab");
    } catch (Exception e) {
      stderr.writeln("TFileWriterTransport: Error on opening output file " ~
        "in writer thread: %s", e);
      hasIOError = true;
    }

    auto flushTimer = StopWatch(AutoStart.yes);
    size_t unflushedByteCount;

    bool shutdownRequested;
    while (true) {
      if (shutdownRequested) break;

      bool forceFlush;
      receiveTimeout(maxFlushInterval - flushTimer.peek,
        (immutable(ubyte)[] data) {
          while (hasIOError) {
            stderr.writefln("TFileWriterTransport: Writer thread going to " ~
              "sleep for %s µs due to IO errors",
              ioErrorSleepDuration.fracSec.usecs
            );

            // Sleep for ioErrorSleepDuration, being ready to be interrupted
            // by shutdown requests.
            auto timedOut = receiveTimeout(ioErrorSleepDuration,
              (ShutdownMessage msg){});
            if (!timedOut) {
              // We got a shutdown request, just drop all events and exit the
              // main loop as to not block application shutdown with our tries
              // which we must assume to fail.
              break;
            }

            try {
              file = File(path, "ab");
              unflushedByteCount = 0;
              hasIOError = false;
              stderr.writefln("TFileWriterTransport: Output file %s reopened " ~
                "during writer thread error recovery", path);
            } catch (Exception e) {
              stderr.writefln("TFileWriterTransport: Unable to reopen output " ~
                "file %s during writer thread error recovery", path);
            }
          }

          // TODO: Chunk boundary checking.

          // TODO: 2 syscalls here, is this a problem performance-wise?
          // TODO: Probably abyssmal performance on Windows due to rawWrite
          // implementation.
          uint len = cast(uint)data.length;
          file.rawWrite(cast(ubyte[])(&len)[0..1]);
          file.rawWrite(data);

          unflushedByteCount += data.length;
        }, (FlushBytesMessage msg) {
          maxFlushBytes = msg.value;
        }, (FlushIntervalMessage msg) {
          maxFlushInterval = msg.value;
        }, (IoErrorSleepDurationMessage msg) {
          ioErrorSleepDuration = msg.value;
        }, (FlushMessage msg) {
          forceFlush = true;
        }, (OwnerTerminated msg) {
          shutdownRequested = true;
        }, (ShutdownMessage msg) {
          shutdownRequested = true;
        }
      );

      if (hasIOError) continue;

      if (forceFlush || shutdownRequested ||
        cast(Duration)flushTimer.peek > maxFlushInterval ||
        unflushedByteCount > maxFlushBytes
      ) {
        file.flush();
        flushTimer.reset();
        if (forceFlush) send(owner, FlushMessage(), thisTid());
      }
    }

    send(owner, ShutdownMessage(), thisTid());
  }
}

version (unittest) {
  import core.memory : GC;
  import std.file;

  version (linux) {
    class CallLog {
      void logCall() {
        calls ~= Clock.currAppTick();
      }

      TickDuration[] calls;
    }

    __gshared CallLog fsyncLog;

    extern(C) int fsync(int fd) {
      stderr.writeln("fsync shim called.");
      if (fsyncLog) {
        fsyncLog.logCall();
      }
      return 0;
    }
  }
}

unittest {
  void tryRemove(string fileName) {
    try {
      remove(fileName);
    } catch (Exception) {}
  }

  /*
   * Check the most basic reading/writing operations.
   */
  {
    immutable fileName = "unittest.dat.tmp";
    scope (exit) tryRemove(fileName);

    auto writer = new TFileWriterTransport(fileName);
    writer.open();
    scope (exit) writer.close();

    writer.write([1, 2]);
    writer.write([3, 4]);
    writer.write([5, 6, 7]);
    writer.flush();

    auto reader = new TFileReaderTransport(fileName);
    reader.open();
    scope (exit) reader.close();

    auto buf = new ubyte[7];
    reader.readAll(buf);
    assert(buf == [1, 2, 3, 4, 5, 6, 7]);
  }

  /*
   * Make sure that close() exits "quickly", i.e. that there is no problem
   * with the worker thread waking up.
   */
  {
    enum NUM_ITERATIONS = 1000;

    uint numOver = 0;
    foreach (n; 0 .. NUM_ITERATIONS) {
      immutable fileName = "unittest.dat.tmp";
      scope (exit) tryRemove(fileName);

      auto transport = new TFileWriterTransport(fileName);
      transport.open();

      // Write something so that the writer thread gets started.
      transport.write(cast(ubyte[])"foo");

      // Every other iteration, also call flush(), just in case that potentially
      // has any effect on how the writer thread wakes up.
      if (n & 0x1) {
        transport.flush();
      }

      // Time the call to close().
      auto sw = StopWatch(AutoStart.yes);
      transport.close();
      sw.stop();

      // If any attempt takes more than 100ms, treat that as a failure.
      // Treat this as a fatal failure, so we'll return now instead of
      // looping over a very slow operation.
      assert(sw.peek.msecs < 100);

      // Normally, it takes less than 1ms on my dev box.
      // However, if the box is heavily loaded, some of the test runs
      // take longer, since we're just waiting for our turn on the CPU.
      if (sw.peek.msecs > 1) {
        ++numOver;
      }

      // Force garbage collection runs every now and then to make sure we
      // don't run out of OS thread handles.
      if (!(n % 100)) GC.collect();
    }

    // Make sure fewer than 10% of the runs took longer than 1000us
    assert(numOver < NUM_ITERATIONS / 10);
  }

  /*
   * Make sure setFlushMaxUs() is honored. These tests are Linux-only, as
   * hooking fsync only works there.
   */
  version (linux) {
    void testMaxFlushDuration(Duration flushDuration, Duration writeDuration,
      Duration testDuration)
    {
      // TFileTransport only calls fsync() if data has been written,
      // so make sure the write interval is smaller than the flush interval.
      assert(writeDuration < flushDuration);

      // Record calls to fsync().
      fsyncLog = new CallLog;

      immutable fileName = "unittest.dat.tmp";
      scope (exit) tryRemove(fileName);

      auto transport = new TFileWriterTransport(fileName);
      transport.open();

      // Don't flush because of # of bytes written
      transport.maxFlushBytes = transport.maxFlushBytes.max;

      // Set the flush interval
      transport.maxFlushInterval = flushDuration;

      // Make one call to write, to start the writer thread now.
      // (If we just let the thread get created during our test loop,
      // the thread creation sometimes takes long enough to make the first
      // fsync interval fail the check.)
      auto buf = cast(ubyte[])"a";
      transport.write(buf);

      // Add one entry to the fsync log, just to mark the start time
      fsyncLog.logCall();

      // Loop doing write(), sleep(), ...
      Duration elapsedTime;
      while (true) {
        transport.write(buf);
        if (elapsedTime > testDuration) {
          break;
        }
        Thread.sleep(writeDuration);
        elapsedTime += writeDuration;
      }

      transport.close();

      // Stop logging new fsync() calls
      auto calls = fsyncLog.calls;
      fsyncLog = null;

      // Examine the fsync() log, allowing for a bit of leeway.
      auto maxAllowedDelta = flushDuration + dur!"msecs"(5);

      // We added 1 fsync call above.
      // Make sure TFileTransport called fsync at least once
      assert(calls.length > 1);

      TickDuration prevTime;
      foreach (i, time; calls) {
        if (i > 0) {
          assert(cast(Duration)(time - prevTime) < maxAllowedDelta);
        }
        prevTime = time;
      }
    }

    // fsync every 10ms, write every 5ms, for 500ms
    testMaxFlushDuration(dur!"msecs"(10), dur!"msecs"(5), dur!"msecs"(500));

    // fsync every 50ms, write every 20ms, for 500ms
    testMaxFlushDuration(dur!"msecs"(50), dur!"msecs"(20), dur!"msecs"(500));

    // fsync every 400ms, write every 300ms, for 1s
    testMaxFlushDuration(dur!"msecs"(400), dur!"msecs"(300), dur!"seconds"(1000));
  }
}
