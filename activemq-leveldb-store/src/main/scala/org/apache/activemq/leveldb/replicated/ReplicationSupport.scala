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
package org.apache.activemq.leveldb.replicated

import org.fusesource.hawtbuf.Buffer._
import java.util.concurrent._
import java.nio.MappedByteBuffer
import sun.nio.ch.DirectBuffer
import java.io.{RandomAccessFile, File}
import java.nio.channels.FileChannel
import java.util.concurrent.atomic.AtomicInteger
import org.fusesource.hawtdispatch._

object ReplicationSupport {

  val WAL_ACTION = ascii("wal")
  val LOGIN_ACTION= ascii("LevelDB Store Replication v1:login")
  val SYNC_ACTION = ascii("sync")
  val GET_ACTION = ascii("get")
  val ACK_ACTION = ascii("ack")
  val OK_ACTION = ascii("ok")
  val DISCONNECT_ACTION = ascii("disconnect")
  val ERROR_ACTION = ascii("error")

  def unmap(buffer:MappedByteBuffer ) {
    try {
      buffer.asInstanceOf[DirectBuffer].cleaner().clean();
    } catch {
      case ignore:Throwable =>
    }
  }

  def map(file:File, offset:Long, length:Long, readOnly:Boolean) = {
    val raf = new RandomAccessFile(file, if(readOnly) "r" else "rw");
    try {
      val mode = if (readOnly) FileChannel.MapMode.READ_ONLY else FileChannel.MapMode.READ_WRITE
      raf.getChannel().map(mode, offset, length);
    } finally {
      raf.close();
    }
  }

  case class RetainedLatch() {

    private val latch = new CountDownLatch(1)
    private val remaining = new AtomicInteger(1)
    private val release_task = ^{ release }

    def retain = {
      remaining.incrementAndGet()
      release_task
    }

    def release {
      if (remaining.decrementAndGet() == 0) {
        latch.countDown()
      }
    }

    def await() = latch.await()
  }

}
