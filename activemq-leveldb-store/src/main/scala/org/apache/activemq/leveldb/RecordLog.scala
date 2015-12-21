/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.leveldb

import java.{lang=>jl}
import java.{util=>ju}

import java.util.zip.CRC32
import java.util.Map.Entry
import java.util.concurrent.atomic.{AtomicInteger, AtomicLong}
import java.io._
import org.fusesource.hawtbuf.{DataByteArrayInputStream, DataByteArrayOutputStream, Buffer}
import org.fusesource.hawtdispatch.BaseRetained
import org.apache.activemq.leveldb.util.FileSupport._
import org.apache.activemq.util.LRUCache
import util.TimeMetric._
import util.{TimeMetric, Log}
import java.util.TreeMap
import java.util.concurrent.locks.{ReentrantReadWriteLock, ReadWriteLock}
import java.util.concurrent.CountDownLatch

object RecordLog extends Log {

  // The log files contain a sequence of variable length log records:
  // record := header + data
  //
  // header :=
  //   '*'      : int8       // Start of Record Magic
  //   kind     : int8       // Help identify content type of the data.
  //   checksum : uint32     // crc32c of the data[]
  //   length   : uint32     // the length the the data

  val LOG_HEADER_PREFIX = '*'.toByte
  val UOW_END_RECORD = -1.toByte

  val LOG_HEADER_SIZE = 10

  val BUFFER_SIZE = 1024*512
  val BYPASS_BUFFER_SIZE = 1024*16

  case class LogInfo(file:File, position:Long, length:Long) {
    def limit = position+length
  }

  def encode_long(a1:Long) = {
    val out = new DataByteArrayOutputStream(8)
    out.writeLong(a1)
    out.toBuffer
  }

  def decode_long(value:Buffer):Long = {
    val in = new DataByteArrayInputStream(value)
    in.readLong()
  }

}

class SuspendCallSupport {

  val lock = new ReentrantReadWriteLock()
  var resumeLatch:CountDownLatch = _
  var resumedLatch:CountDownLatch = _
  @volatile
  var threads = new AtomicInteger()

  def suspend = this.synchronized {
    val suspended = new CountDownLatch(1)
    resumeLatch = new CountDownLatch(1)
    resumedLatch = new CountDownLatch(1)
    new Thread("Suspend Lock") {
      override def run = {
        try {
          lock.writeLock().lock()
          suspended.countDown()
          resumeLatch.await()
        } finally {
          lock.writeLock().unlock();
          resumedLatch.countDown()
        }
      }
    }.start()
    suspended.await()
  }

  def resume = this.synchronized {
    if( resumedLatch != null ) {
      resumeLatch.countDown()
      resumedLatch.await();
      resumeLatch = null
      resumedLatch = null
    }
  }

  def call[T](func: =>T):T= {
    threads.incrementAndGet()
    lock.readLock().lock()
    try {
      func
    } finally {
      threads.decrementAndGet()
      lock.readLock().unlock()
    }
  }

}

class RecordLogTestSupport {

  val forceCall = new SuspendCallSupport()
  val writeCall = new SuspendCallSupport()
  val deleteCall = new SuspendCallSupport()

}

case class RecordLog(directory: File, logSuffix:String) {
  import RecordLog._

  directory.mkdirs()

  var logSize = 1024 * 1024 * 100L
  var current_appender:LogAppender = _
  var verify_checksums = false
  val log_infos = new TreeMap[Long, LogInfo]()

  var recordLogTestSupport:RecordLogTestSupport =
    if( java.lang.Boolean.getBoolean("org.apache.activemq.leveldb.test") ) {
      new RecordLogTestSupport()
    } else {
      null
    }


  object log_mutex

  def delete(id:Long) = {
    log_mutex.synchronized {
      // We can't delete the current appender.
      if( current_appender.position != id ) {
        Option(log_infos.get(id)).foreach { info =>
          onDelete(info.file)
          onDelete(id)
          log_infos.remove(id)
          reader_cache.synchronized {
            val reader = reader_cache.remove(info.file);
            if( reader!=null ) {
              reader.release();
            }
          }
        }
      }
    }
  }

  protected def onDelete(file:Long) = {
  }

  protected def onDelete(file:File) = {
    if( recordLogTestSupport!=null ) {
      recordLogTestSupport.deleteCall.call {
        file.delete()
      }
    } else {
      file.delete()
    }
  }

  def checksum(data: Buffer): Int = {
    val checksum = new CRC32
    checksum.update(data.data, data.offset, data.length)
    (checksum.getValue & 0xFFFFFFFF).toInt
  }

  class LogAppender(file:File, position:Long, var append_offset:Long=0L) extends LogReader(file, position) {

    val info = new LogInfo(file, position, 0)

    override def open = new RandomAccessFile(file, "rw")

    override def on_close ={
      force
    }

    val flushed_offset = new AtomicLong(append_offset)

    def append_position = {
      position+append_offset
    }

    // set the file size ahead of time so that we don't have to sync the file
    // meta-data on every log sync.
    if( append_offset==0 ) {
      channel.position(logSize-1)
      channel.write(new Buffer(1).toByteBuffer)
      channel.force(true)
      channel.position(0)
    }

    val write_buffer = new DataByteArrayOutputStream(BUFFER_SIZE+LOG_HEADER_SIZE)

    def force = {
      flush
      max_log_flush_latency {
        // only need to update the file metadata if the file size changes..
        if( recordLogTestSupport!=null ) {
          recordLogTestSupport.forceCall.call {
            channel.force(append_offset > logSize)
          }
        } else {
          channel.force(append_offset > logSize)
        }
      }
    }


    def skip(length:Long) = this.synchronized {
      flush
      append_offset += length
      flushed_offset.addAndGet(length)
    }

    /**
     * returns the offset position of the data record.
     */
    def append(id:Byte, data: Buffer) = this.synchronized {
      val record_position = append_position
      val data_length = data.length
      val total_length = LOG_HEADER_SIZE + data_length

      if( write_buffer.position() + total_length > BUFFER_SIZE ) {
        flush
      }

      val cs: Int = checksum(data)
//      trace("Writing at: "+record_position+" len: "+data_length+" with checksum: "+cs)

      if( false && total_length > BYPASS_BUFFER_SIZE ) {

        // Write the header and flush..
        write_buffer.writeByte(LOG_HEADER_PREFIX)
        write_buffer.writeByte(id)
        write_buffer.writeInt(cs)
        write_buffer.writeInt(data_length)

        append_offset += LOG_HEADER_SIZE
        flush

        // Directly write the data to the channel since it's large.
        val buffer = data.toByteBuffer
        val pos = append_offset+LOG_HEADER_SIZE
        val remaining = buffer.remaining

        if( recordLogTestSupport!=null ) {
          recordLogTestSupport.writeCall.call {
            channel.write(buffer, pos)
          }
        } else {
          channel.write(buffer, pos)
        }

        flushed_offset.addAndGet(remaining)
        if( buffer.hasRemaining ) {
          throw new IOException("Short write")
        }
        append_offset += data_length

      } else {
        write_buffer.writeByte(LOG_HEADER_PREFIX)
        write_buffer.writeByte(id)
        write_buffer.writeInt(cs)
        write_buffer.writeInt(data_length)
        write_buffer.write(data.data, data.offset, data_length)
        append_offset += total_length
      }
      (record_position, info)
    }

    def flush = max_log_flush_latency { this.synchronized {
      if( write_buffer.position() > 0 ) {
        val buffer = write_buffer.toBuffer.toByteBuffer
        val remaining = buffer.remaining
        val pos = append_offset-remaining

        if( recordLogTestSupport!=null ) {
          recordLogTestSupport.writeCall.call {
            channel.write(buffer, pos)
          }
        } else {
          channel.write(buffer, pos)
        }

        flushed_offset.addAndGet(remaining)
        if( buffer.hasRemaining ) {
          throw new IOException("Short write")
        }
        write_buffer.reset()
      } }
    }

    override def check_read_flush(end_offset:Long) = {
      if( flushed_offset.get() < end_offset )  {
        flush
      }
    }

  }

  case class LogReader(file:File, position:Long) extends BaseRetained {

    def open = new RandomAccessFile(file, "r")

    val fd = open
    val channel = fd.getChannel

    override def dispose() {
      on_close
      fd.close()
    }

    def on_close = {}

    def check_read_flush(end_offset:Long) = {}

    def read(record_position:Long, length:Int) = {
      val offset = record_position-position
      assert(offset >=0 )

      check_read_flush(offset+LOG_HEADER_SIZE+length)

      if(verify_checksums) {

        val record = new Buffer(LOG_HEADER_SIZE+length)

        def record_is_not_changing = {
          using(open) { fd =>
            val channel = fd.getChannel
            val new_record = new Buffer(LOG_HEADER_SIZE+length)
            channel.read(new_record.toByteBuffer, offset)
            var same = record == new_record
            println(same)
            same
          }
        }

        if( channel.read(record.toByteBuffer, offset) != record.length ) {
          assert( record_is_not_changing )
          throw new IOException("short record at position: "+record_position+" in file: "+file+", offset: "+offset)
        }

        val is = new DataByteArrayInputStream(record)
        val prefix = is.readByte()
        if( prefix != LOG_HEADER_PREFIX ) {
          assert(record_is_not_changing)
          throw new IOException("invalid record at position: "+record_position+" in file: "+file+", offset: "+offset)
        }

        val id = is.readByte()
        val expectedChecksum = is.readInt()
        val expectedLength = is.readInt()
        val data = is.readBuffer(length)

        // If your reading the whole record we can verify the data checksum
        if( expectedLength == length ) {
          if( expectedChecksum != checksum(data) ) {
            assert(record_is_not_changing)
            throw new IOException("checksum does not match at position: "+record_position+" in file: "+file+", offset: "+offset)
          }
        }

        data
      } else {
        val data = new Buffer(length)
        var bb = data.toByteBuffer
        var position = offset+LOG_HEADER_SIZE
        while( bb.hasRemaining  ) {
          var count = channel.read(bb, position)
          if( count == 0 ) {
            throw new IOException("zero read at file '%s' offset: %d".format(file, position))
          }
          if( count < 0 ) {
            throw new EOFException("File '%s' offset: %d".format(file, position))
          }
          position += count
        }
        data
      }
    }

    def read(record_position:Long) = {
      val offset = record_position-position
      val header = new Buffer(LOG_HEADER_SIZE)
      check_read_flush(offset+LOG_HEADER_SIZE)
      channel.read(header.toByteBuffer, offset)
      val is = header.bigEndianEditor();
      val prefix = is.readByte()
      if( prefix != LOG_HEADER_PREFIX ) {
        // Does not look like a record.
        throw new IOException("invalid record position %d (file: %s, offset: %d)".format(record_position, file.getAbsolutePath, offset))
      }
      val id = is.readByte()
      val expectedChecksum = is.readInt()
      val length = is.readInt()
      val data = new Buffer(length)

      check_read_flush(offset+LOG_HEADER_SIZE+length)
      if( channel.read(data.toByteBuffer, offset+LOG_HEADER_SIZE) != length ) {
        throw new IOException("short record")
      }

      if(verify_checksums) {
        if( expectedChecksum != checksum(data) ) {
          throw new IOException("checksum does not match")
        }
      }
      (id, data, record_position+LOG_HEADER_SIZE+length)
    }

    def check(record_position:Long):Option[(Long, Option[Long])] = {
      var offset = record_position-position
      val header = new Buffer(LOG_HEADER_SIZE)
      channel.read(header.toByteBuffer, offset)
      val is = header.bigEndianEditor();
      val prefix = is.readByte()
      if( prefix != LOG_HEADER_PREFIX ) {
        return None // Does not look like a record.
      }
      val kind = is.readByte()
      val expectedChecksum = is.readInt()
      val length = is.readInt()

      val chunk = new Buffer(1024*4)
      val chunkbb = chunk.toByteBuffer
      offset += LOG_HEADER_SIZE

      // Read the data in in chunks to avoid
      // OOME if we are checking an invalid record
      // with a bad record length
      val checksumer = new CRC32
      var remaining = length
      while( remaining > 0 ) {
        val chunkSize = remaining.min(1024*4);
        chunkbb.position(0)
        chunkbb.limit(chunkSize)
        channel.read(chunkbb, offset)
        if( chunkbb.hasRemaining ) {
          return None
        }
        checksumer.update(chunk.data, 0, chunkSize)
        offset += chunkSize
        remaining -= chunkSize
      }

      val checksum = ( checksumer.getValue & 0xFFFFFFFF).toInt
      if( expectedChecksum !=  checksum ) {
        return None
      }
      val uow_start_pos = if(kind == UOW_END_RECORD && length==8) Some(decode_long(chunk)) else None
      return Some(record_position+LOG_HEADER_SIZE+length, uow_start_pos)
    }

    def verifyAndGetEndOffset:Long = {
      var pos = position;
      var current_uow_start = pos
      val limit = position+channel.size()
      while(pos < limit) {
        check(pos) match {
          case Some((next, uow_start_pos)) =>
            uow_start_pos.foreach { uow_start_pos =>
              if( uow_start_pos == current_uow_start ) {
                current_uow_start = next
              } else {
                return current_uow_start-position
              }
            }
            pos = next
          case None =>
            return current_uow_start-position
        }
      }
      return current_uow_start-position
    }
  }

  def create_log_appender(position: Long, offset:Long) = {
    new LogAppender(next_log(position), position, offset)
  }

  def create_appender(position: Long, offset:Long): Any = {
    log_mutex.synchronized {
      if(current_appender!=null) {
        log_infos.put (position, new LogInfo(current_appender.file, current_appender.position, current_appender.append_offset))
      }
      current_appender = create_log_appender(position, offset)
      log_infos.put(position, new LogInfo(current_appender.file, position, 0))
    }
  }

  val max_log_write_latency = TimeMetric()
  val max_log_flush_latency = TimeMetric()
  val max_log_rotate_latency = TimeMetric()

  def open(appender_size:Long= -1) = {
    log_mutex.synchronized {
      log_infos.clear()
      LevelDBClient.find_sequence_files(directory, logSuffix).foreach { case (position,file) =>
        log_infos.put(position, LogInfo(file, position, file.length()))
      }

      if( log_infos.isEmpty ) {
        create_appender(0,0)
      } else {
        val file = log_infos.lastEntry().getValue
        if( appender_size == -1 ) {
          val r = LogReader(file.file, file.position)
          try {
            val endOffset = r.verifyAndGetEndOffset
            using(new RandomAccessFile(file.file, "rw")) { file=>
              try {
                file.getChannel.truncate(endOffset)
              }
              catch {
                case e:Throwable =>
                  e.printStackTrace()
              }
              file.getChannel.force(true)
            }
            create_appender(file.position,endOffset)
          } finally {
            r.release()
          }
        } else {
          create_appender(file.position,appender_size)
        }
      }
    }
  }

  def isOpen = {
    log_mutex.synchronized {
      current_appender!=null;
    }
  }

  def close = {
    log_mutex.synchronized {
      if( current_appender!=null ) {
        current_appender.release
      }
    }
  }

  def appender_limit = current_appender.append_position
  def appender_start = current_appender.position

  def next_log(position:Long) = LevelDBClient.create_sequence_file(directory, position, logSuffix)

  def appender[T](func: (LogAppender)=>T):T= {
    val intial_position = current_appender.append_position
    try {
      max_log_write_latency {
        val rc = func(current_appender)
        if( current_appender.append_position != intial_position ) {
          // Record a UOW_END_RECORD so that on recovery we only replay full units of work.
          current_appender.append(UOW_END_RECORD,encode_long(intial_position))
        }
        rc
      }
    } finally {
      current_appender.flush
      max_log_rotate_latency {
        log_mutex.synchronized {
          if ( current_appender.append_offset >= logSize ) {
            rotate
          }
        }
      }
    }
  }


  def rotate[T] = log_mutex.synchronized {
    current_appender.release()
    on_log_rotate()
    create_appender(current_appender.append_position, 0)
  }

  var on_log_rotate: ()=>Unit = ()=>{}

  private val reader_cache = new LRUCache[File, LogReader](100) {
    protected override def onCacheEviction(entry: Entry[File, LogReader]) = {
      entry.getValue.release()
    }
  }

  def log_info(pos:Long) = log_mutex.synchronized { Option(log_infos.floorEntry(pos)).map(_.getValue) }

  def log_file_positions = log_mutex.synchronized {
    import collection.JavaConversions._
    log_infos.map(_._2.position).toArray
  }

  private def get_reader[T](record_position:Long)(func: (LogReader)=>T):Option[T] = {

    val (info, appender) = log_mutex.synchronized {
      log_info(record_position) match {
        case None =>
          warn("No reader available for position: %x, log_infos: %s", record_position, log_infos)
          return None
        case Some(info) =>
          if(info.position == current_appender.position) {
            current_appender.retain()
            (info, current_appender)
          } else {
            (info, null)
          }
      }
    }

    val reader = if( appender!=null ) {
      // read from the current appender.
      appender
    } else {
      // Checkout a reader from the cache...
      reader_cache.synchronized {
        var reader = reader_cache.get(info.file)
        if(reader==null) {
          reader = LogReader(info.file, info.position)
          reader_cache.put(info.file, reader)
        }
        reader.retain()
        reader
      }
    }

    try {
      Some(func(reader))
    } finally {
      reader.release
    }
  }

  def read(pos:Long) = {
    get_reader(pos)(_.read(pos))
  }
  def read(pos:Long, length:Int) = {
    get_reader(pos)(_.read(pos, length))
  }

}
