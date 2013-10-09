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


import org.apache.activemq.leveldb.util._

import FileSupport._
import java.io._
import org.apache.activemq.leveldb.{RecordLog, LevelDBClient}
import java.util
import org.apache.activemq.leveldb.replicated.dto.{SyncResponse, FileInfo}

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object MasterLevelDBClient extends Log {

  val MANIFEST_SUFFIX = ".mf"
  val LOG_SUFFIX = LevelDBClient.LOG_SUFFIX
  val INDEX_SUFFIX = LevelDBClient.INDEX_SUFFIX

}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class MasterLevelDBClient(val store:MasterLevelDBStore) extends LevelDBClient(store) {
  import MasterLevelDBClient._
  import collection.JavaConversions._

  var snapshots_pending_delete = new util.TreeSet[Long]()

  def slave_held_snapshots = {
    val rc = new util.HashSet[Long]()
    for( v <- store.slaves.values() ; s <- v.held_snapshot ) {
      rc.add(s)
    }
    rc
  }

  override def replaceLatestSnapshotDirectory(newSnapshotIndexPos: Long) {
    if( slave_held_snapshots.contains(lastIndexSnapshotPos) ) {
      // is a slave is holding open a snapshot.. lets not delete it's data just yet...
      snapshots_pending_delete.add(newSnapshotIndexPos)
      lastIndexSnapshotPos = newSnapshotIndexPos
    } else {
      super.replaceLatestSnapshotDirectory(newSnapshotIndexPos)
    }
  }

  override def gc(topicPositions: Seq[(Long, Long)]) {
    val snapshots_to_rm = new util.HashSet(snapshots_pending_delete)
    snapshots_to_rm.removeAll(slave_held_snapshots);

    for ( snapshot <- snapshots_to_rm ) {
      snapshotIndexFile(snapshot).recursiveDelete
    }
    super.gc(topicPositions)
  }

  override def oldest_retained_snapshot: Long = {
    if ( snapshots_pending_delete.isEmpty ) {
      super.oldest_retained_snapshot
    } else {
      snapshots_pending_delete.first()
    }
  }

  def snapshot_state(snapshot_id:Long) = {
    def info(file:File) = {
      val rc = new FileInfo
      rc.file = file.getName
      rc.length = file.length()
      rc
    }

    val rc = new SyncResponse
    rc.snapshot_position = snapshot_id
    rc.wal_append_position = log.current_appender.append_position

    for( file <- logDirectory.listFiles; if file.getName.endsWith(LOG_SUFFIX) ) {
      // Only need to sync up to what's been flushed.
      val fileInfo = info(file)
      if( log.current_appender.file == file ) {
        rc.append_log = file.getName
        fileInfo.length = log.current_appender.flushed_offset.get()
        fileInfo.crc32 = file.crc32(fileInfo.length)
      } else {
        fileInfo.crc32 = file.cached_crc32
      }
      rc.log_files.add(fileInfo)
    }

    val index_dir = LevelDBClient.create_sequence_file(directory, snapshot_id, INDEX_SUFFIX)
    if( index_dir.exists() ) {
      for( file <- index_dir.listFiles ) {
        val name = file.getName
        if( name !="LOCK" ) {
          rc.index_files.add(info(file))
        }
      }
    }

    rc
  }


  // Override the log appender implementation so that it
  // stores the logs on the local and remote file systems.
  override def createLog = new RecordLog(directory, LOG_SUFFIX) {

    override def create_log_appender(position: Long, offset:Long) = {
      new LogAppender(next_log(position), position, offset) {

        val file_name = file.getName

        override def flush = this.synchronized {
          val offset = flushed_offset.get()
          super.flush
          val length = flushed_offset.get() - offset;
          store.replicate_wal(file, position, offset, length)
        }

        override def force = {
          import MasterLevelDBStore._
          if( (store.syncToMask & SYNC_TO_DISK) != 0) {
            super.force
          }
          if( (store.syncToMask & SYNC_TO_REMOTE) != 0) {
            flush
            store.wal_sync_to(position+flushed_offset.get())
          }
        }

        override def on_close {
          super.force
        }
      }
    }

    override protected def onDelete(file: Long) = {
      super.onDelete(file)
      store.replicate_log_delete(file)
    }
  }
}
