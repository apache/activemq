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

package org.apache.activemq.leveldb.dfs

import org.apache.activemq.leveldb.util._

import org.fusesource.leveldbjni.internal.Util
import FileSupport._
import java.io._
import scala.collection.mutable._
import scala.collection.immutable.TreeMap
import org.fusesource.hawtbuf.Buffer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.activemq.leveldb.{RecordLog, LevelDBClient}
import scala.Some


/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object DFSLevelDBClient extends Log {

  val MANIFEST_SUFFIX = ".mf"
  val LOG_SUFFIX = LevelDBClient.LOG_SUFFIX
  val INDEX_SUFFIX = LevelDBClient.INDEX_SUFFIX


  def create_sequence_path(directory:Path, id:Long, suffix:String) = new Path(directory, ("%016x%s".format(id, suffix)))

  def find_sequence_status(fs:FileSystem, directory:Path, suffix:String) = {
    TreeMap((fs.listStatus(directory).flatMap { f =>
      val name = f.getPath.getName
      if( name.endsWith(suffix) ) {
        try {
          val base = name.stripSuffix(suffix)
          val position = java.lang.Long.parseLong(base, 16);
          Some(position -> f )
        } catch {
          case e:NumberFormatException => None
        }
      } else {
        None
      }
    }): _* )
  }

}

/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DFSLevelDBClient(val store:DFSLevelDBStore) extends LevelDBClient(store) {
  import DFSLevelDBClient._

  case class Snapshot(current_manifest:String, files:Set[String])
  var snapshots = TreeMap[Long, Snapshot]()

  // Eventually we will allow warm standby slaves to add references to old
  // snapshots so that we don't delete them while they are in the process
  // of downloading the snapshot.
  var snapshotRefCounters = HashMap[Long, LongCounter]()
  var indexFileRefCounters = HashMap[String, LongCounter]()

  def dfs = store.dfs
  def dfsDirectory = new Path(store.dfsDirectory)
  def dfsBlockSize = store.dfsBlockSize
  def dfsReplication = store.dfsReplication
  def remoteIndexPath = new Path(dfsDirectory, "index")

  override def start() = {
    might_fail {
      directory.mkdirs()
      dfs.mkdirs(dfsDirectory)
      downloadLogFiles
      dfs.mkdirs(remoteIndexPath)
      downloadIndexFiles
    }
    super.start()
    storeTrace("Master takeover by: "+store.containerId, true)
  }

  override def locked_purge = {
    super.locked_purge
    dfs.delete(dfsDirectory, true)
  }

  override def snapshotIndex(sync: Boolean) = {
    val previous_snapshot = lastIndexSnapshotPos
    super.snapshotIndex(sync)
    // upload the snapshot to the dfs
    uploadIndexFiles(lastIndexSnapshotPos)

    // Drop the previous snapshot reference..
    for( counter <- snapshotRefCounters.get(previous_snapshot)) {
      if( counter.decrementAndGet() <= 0 ) {
        snapshotRefCounters.remove(previous_snapshot)
      }
    }
    gcSnapshotRefs
  }

  // downloads missing log files...
  def downloadLogFiles {
    val log_files = find_sequence_status(dfs, dfsDirectory, LOG_SUFFIX)
    val downloads = log_files.flatMap( _ match {
      case (id, status) =>
        val target = LevelDBClient.create_sequence_file(directory, id, LOG_SUFFIX)
        // is it missing or does the size not match?
        if (!target.exists() || target.length() != status.getLen) {
          Some((id, status))
        } else {
          None
        }
    })
    if( !downloads.isEmpty ) {
      val total_size = downloads.foldLeft(0L)((a,x)=> a+x._2.getLen)
      downloads.foreach {
        case (id, status) =>
          val target = LevelDBClient.create_sequence_file(directory, id, LOG_SUFFIX)
          // is it missing or does the size not match?
          if (!target.exists() || target.length() != status.getLen) {
            info("Downloading log file: "+status.getPath.getName)
            using(dfs.open(status.getPath, 32*1024)) { is=>
              using(new FileOutputStream(target)) { os=>
                copy(is, os)
              }
            }
          }
      }
    }
  }

  // See if there is a more recent index that can be downloaded.
  def downloadIndexFiles {

    snapshots = TreeMap()
    dfs.listStatus(remoteIndexPath).foreach { status =>
      val name = status.getPath.getName
      indexFileRefCounters.put(name, new LongCounter())
      if( name endsWith MANIFEST_SUFFIX ) {
        info("Getting index snapshot manifest: "+status.getPath.getName)
        val mf = using(dfs.open(status.getPath)) { is =>
          JsonCodec.decode(is, classOf[IndexManifestDTO])
        }
        import collection.JavaConversions._
        snapshots += mf.snapshot_id -> Snapshot(mf.current_manifest, Set(mf.files.toSeq:_*))
      }
    }

    // Check for invalid snapshots..
    for( (snapshotid, snapshot) <- snapshots) {
      val matches = indexFileRefCounters.keySet & snapshot.files
      if( matches.size != snapshot.files.size ) {
        var path = create_sequence_path(remoteIndexPath, snapshotid, MANIFEST_SUFFIX)
        warn("Deleting inconsistent snapshot manifest: "+path.getName)
        dfs.delete(path, true)
        snapshots -= snapshotid
      }
    }

    // Add a ref to the last snapshot..
    for( (snapshotid, _) <- snapshots.lastOption ) {
      snapshotRefCounters.getOrElseUpdate(snapshotid, new LongCounter()).incrementAndGet()
    }
    
    // Increment index file refs..
    for( key <- snapshotRefCounters.keys; snapshot <- snapshots.get(key); file <- snapshot.files ) {
      indexFileRefCounters.getOrElseUpdate(file, new LongCounter()).incrementAndGet()
    }

    // Remove un-referenced index files.
    for( (name, counter) <- indexFileRefCounters ) {
      if( counter.get() <= 0 ) {
        var path = new Path(remoteIndexPath, name)
        info("Deleting unreferenced index file: "+path.getName)
        dfs.delete(path, true)
        indexFileRefCounters.remove(name)
      }
    }

    val local_snapshots = Map(LevelDBClient.find_sequence_files(directory, INDEX_SUFFIX).values.flatten { dir =>
      if( dir.isDirectory ) dir.listFiles() else Array[File]()
    }.map(x=> (x.getName, x)).toSeq:_*)

    for( (id, snapshot) <- snapshots.lastOption ) {

      // increment the ref..
      tempIndexFile.recursiveDelete
      tempIndexFile.mkdirs

      for( file <- snapshot.files ; if !file.endsWith(MANIFEST_SUFFIX) ) {
        val target = tempIndexFile / file

        // The file might be in a local snapshot already..
        local_snapshots.get(file) match {
          case Some(f) =>
            // had it locally.. link it.
            Util.link(f, target)
          case None =>
            // download..
            var path = new Path(remoteIndexPath, file)
            info("Downloading index file: "+path)
            using(dfs.open(path, 32*1024)) { is=>
              using(new FileOutputStream(target)) { os=>
                copy(is, os)
              }
            }
        }
      }

      val current = tempIndexFile / "CURRENT"
      current.writeText(snapshot.current_manifest)

      // We got everything ok, now rename.
      tempIndexFile.renameTo(LevelDBClient.create_sequence_file(directory, id, INDEX_SUFFIX))
    }

    gcSnapshotRefs
  }

  def gcSnapshotRefs = {
    snapshots = snapshots.filter { case (id, snapshot)=>
      if (snapshotRefCounters.get(id).isDefined) {
        true
      } else {
        for( file <- snapshot.files ) {
          for( counter <- indexFileRefCounters.get(file) ) {
            if( counter.decrementAndGet() <= 0 ) {
              var path = new Path(remoteIndexPath, file)
              info("Deleteing unreferenced index file: %s", path.getName)
              dfs.delete(path, true)
              indexFileRefCounters.remove(file)
            }
          }
        }
        false
      }
    }
  }

  def uploadIndexFiles(snapshot_id:Long):Unit = {

    val source = LevelDBClient.create_sequence_file(directory, snapshot_id, INDEX_SUFFIX)
    try {

      // Build the new manifest..
      val mf = new IndexManifestDTO
      mf.snapshot_id = snapshot_id
      mf.current_manifest = (source / "CURRENT").readText()
      source.listFiles.foreach { file =>
        val name = file.getName
        if( name !="LOCK" && name !="CURRENT") {
          mf.files.add(name)
        }
      }

      import collection.JavaConversions._
      mf.files.foreach { file =>
        val refs = indexFileRefCounters.getOrElseUpdate(file, new LongCounter())
        if(refs.get()==0) {
          // Upload if not not yet on the remote.
          val target = new Path(remoteIndexPath, file)
          using(new FileInputStream(source / file)) { is=>
            using(dfs.create(target, true, 1024*32, dfsReplication.toShort, dfsBlockSize)) { os=>
              copy(is, os)
            }
          }
        }
        refs.incrementAndGet()
      }

      val target = create_sequence_path(remoteIndexPath, mf.snapshot_id, MANIFEST_SUFFIX)
      mf.files.add(target.getName)

      indexFileRefCounters.getOrElseUpdate(target.getName, new LongCounter()).incrementAndGet()
      using(dfs.create(target, true, 1024*32, dfsReplication.toShort, dfsBlockSize)) { os=>
	var outputStream:OutputStream = os.asInstanceOf[OutputStream]
        JsonCodec.mapper.writeValue(outputStream, mf)
      }

      snapshots += snapshot_id -> Snapshot(mf.current_manifest, Set(mf.files.toSeq:_*))
      snapshotRefCounters.getOrElseUpdate(snapshot_id, new LongCounter()).incrementAndGet()

    } catch {
      case e: Exception =>
        warn(e, "Could not upload the index: " + e)
    }
  }



  // Override the log appender implementation so that it
  // stores the logs on the local and remote file systems.
  override def createLog = new RecordLog(directory, LOG_SUFFIX) {


    override protected def onDelete(file: File) = {
      super.onDelete(file)
      // also delete the file on the dfs.
      dfs.delete(new Path(dfsDirectory, file.getName), false)
    }

    override def create_log_appender(position: Long, offset:Long) = {
      new LogAppender(next_log(position), position, offset) {

        val dfs_path = new Path(dfsDirectory, file.getName)
        debug("Opening DFS log file for append: "+dfs_path.getName)
        val dfs_os = dfs.create(dfs_path, true, RecordLog.BUFFER_SIZE, dfsReplication.toShort, dfsBlockSize )
        debug("Opened")

        override def flush = this.synchronized {
          if( write_buffer.position() > 0 ) {

            var buffer: Buffer = write_buffer.toBuffer
            // Write it to DFS..
            buffer.writeTo(dfs_os.asInstanceOf[OutputStream]);

            // Now write it to the local FS.
            val byte_buffer = buffer.toByteBuffer
            val pos = append_offset-byte_buffer.remaining
            flushed_offset.addAndGet(byte_buffer.remaining)
            channel.write(byte_buffer, pos)
            if( byte_buffer.hasRemaining ) {
              throw new IOException("Short write")
            }

            write_buffer.reset()
          }
        }

        override def force = {
          dfs_os.sync()
        }

        override def on_close {
          super.force
          dfs_os.close()
        }
      }
    }
  }
}
