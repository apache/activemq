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
import org.apache.activemq.leveldb.util.FileSupport._
import org.apache.activemq.leveldb.LevelDBClient
import scala.collection.immutable.TreeMap

object ReplicationSupport {

  val WAL_ACTION = ascii("wal")
  val LOGIN_ACTION= ascii("LevelDB Store Replication v1:login")
  val SYNC_ACTION = ascii("sync")
  val GET_ACTION = ascii("get")
  val ACK_ACTION = ascii("ack")
  val OK_ACTION = ascii("ok")
  val DISCONNECT_ACTION = ascii("disconnect")
  val ERROR_ACTION = ascii("error")
  val LOG_DELETE_ACTION = ascii("rm")

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

  def stash(directory:File) {
    directory.mkdirs()
    val tmp_stash = directory / "stash.tmp"
    val stash = directory / "stash"
    stash.recursiveDelete
    tmp_stash.recursiveDelete
    tmp_stash.mkdirs()
    copy_store_dir(directory, tmp_stash)
    tmp_stash.renameTo(stash)
  }

  def copy_store_dir(from:File, to:File) = {
    val log_files = LevelDBClient.find_sequence_files(from, LevelDBClient.LOG_SUFFIX)
    if( !log_files.isEmpty ) {
      val append_file = log_files.last._2
      for( file <- log_files.values ; if file != append_file) {
        file.linkTo(to / file.getName)
        val crc_file = file.getParentFile / (file.getName+".crc32" )
        if( crc_file.exists() ) {
          crc_file.linkTo(to / crc_file.getName)
        }
      }
      append_file.copyTo(to / append_file.getName)
    }

    val index_dirs = LevelDBClient.find_sequence_files(from, LevelDBClient.INDEX_SUFFIX)
    if( !index_dirs.isEmpty ) {
      val index_file = index_dirs.last._2
      var target = to / index_file.getName
      target.mkdirs()
      LevelDBClient.copyIndex(index_file, target)
    }
  }

  def stash_clear(directory:File) {
    val stash = directory / "stash"
    stash.recursiveDelete
  }

  def unstash(directory:File) {
    val tmp_stash = directory / "stash.tmp"
    tmp_stash.recursiveDelete
    val stash = directory / "stash"
    if( stash.exists() ) {
      delete_store(directory)
      copy_store_dir(stash, directory)
      stash.recursiveDelete
    }
  }

  def delete_store(directory: File) {
    // Delete any existing files to make space for the stash we will be restoring..
    var t: TreeMap[Long, File] = LevelDBClient.find_sequence_files(directory, LevelDBClient.LOG_SUFFIX)
    for (entry <- t) {
      val file = entry._2
      file.delete()
      val crc_file = directory / (file.getName+".crc32" )
      if( crc_file.exists() ) {
        crc_file.delete()
      }
    }
    for (file <- LevelDBClient.find_sequence_files(directory, LevelDBClient.INDEX_SUFFIX)) {
      file._2.recursiveDelete
    }
  }
}
