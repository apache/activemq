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

import java.util.concurrent.locks.ReentrantReadWriteLock
import collection.immutable.TreeMap
import collection.mutable.{HashMap, ListBuffer}
import org.iq80.leveldb._

import org.fusesource.hawtdispatch._
import record.{CollectionKey, EntryKey, EntryRecord, CollectionRecord}
import org.apache.activemq.leveldb.util._
import java.util.concurrent._
import org.fusesource.hawtbuf._
import java.io._
import scala.Option._
import org.apache.activemq.command.{MessageAck, Message}
import org.apache.activemq.util.{IOExceptionSupport, ByteSequence}
import java.text.SimpleDateFormat
import java.util.{Date, Collections}
import org.fusesource.leveldbjni.internal.JniDB
import org.apache.activemq.ActiveMQMessageAuditNoSync
import org.apache.activemq.leveldb.util.TimeMetric
import org.fusesource.hawtbuf.ByteArrayInputStream
import org.apache.activemq.leveldb.RecordLog.LogInfo
import scala.Some
import scala.Serializable
import org.fusesource.hawtbuf.ByteArrayOutputStream
import org.apache.activemq.broker.SuppressReplyException

/**
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
object LevelDBClient extends Log {

  class WriteThread(r:Runnable) extends Thread(r) {
    setDaemon(true)
  }

  final val STORE_SCHEMA_PREFIX = "activemq_leveldb_store:"
  final val STORE_SCHEMA_VERSION = 1

  final val THREAD_POOL_STACK_SIZE = System.getProperty("leveldb.thread.stack.size", "" + 1024 * 512).toLong
  final val THREAD_POOL: ThreadPoolExecutor = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue[Runnable], new ThreadFactory {
    def newThread(r: Runnable): Thread = {
      var rc: Thread = new Thread(null, r, "LevelDB Store Task", THREAD_POOL_STACK_SIZE)
      rc.setDaemon(true)
      return rc
    }
  }) {
    override def shutdown: Unit = {}
    override def shutdownNow = Collections.emptyList[Runnable]
  }

  val PLIST_WRITE_OPTIONS = new WriteOptions().sync(false)

  final val DIRTY_INDEX_KEY = bytes(":dirty")
  final val LOG_REF_INDEX_KEY = bytes(":log-refs")
  final val LOGS_INDEX_KEY = bytes(":logs")
  final val PRODUCER_IDS_INDEX_KEY = bytes(":producer_ids")

  final val COLLECTION_META_KEY = bytes(":collection-meta")
  final val TRUE = bytes("true")
  final val FALSE = bytes("false")
  final val ACK_POSITION = new AsciiBuffer("p")

  final val COLLECTION_PREFIX = 'c'.toByte
  final val COLLECTION_PREFIX_ARRAY = Array(COLLECTION_PREFIX)
  final val ENTRY_PREFIX = 'e'.toByte
  final val ENTRY_PREFIX_ARRAY = Array(ENTRY_PREFIX)

  final val LOG_ADD_COLLECTION      = 1.toByte
  final val LOG_REMOVE_COLLECTION   = 2.toByte
  final val LOG_ADD_ENTRY           = 3.toByte
  final val LOG_REMOVE_ENTRY        = 4.toByte
  final val LOG_DATA                = 5.toByte
  final val LOG_TRACE               = 6.toByte
  final val LOG_UPDATE_ENTRY        = 7.toByte

  final val LOG_SUFFIX  = ".log"
  final val INDEX_SUFFIX  = ".index"
  
  implicit def toByteArray(buffer:Buffer) = buffer.toByteArray
  implicit def toBuffer(buffer:Array[Byte]) = new Buffer(buffer)
  
  def encodeCollectionRecord(v: CollectionRecord.Buffer) = v.toUnframedByteArray
  def decodeCollectionRecord(data: Buffer):CollectionRecord.Buffer = CollectionRecord.FACTORY.parseUnframed(data)
  def encodeCollectionKeyRecord(v: CollectionKey.Buffer) = v.toUnframedByteArray
  def decodeCollectionKeyRecord(data: Buffer):CollectionKey.Buffer = CollectionKey.FACTORY.parseUnframed(data)

  def encodeEntryRecord(v: EntryRecord.Buffer) = v.toUnframedBuffer
  def decodeEntryRecord(data: Buffer):EntryRecord.Buffer = EntryRecord.FACTORY.parseUnframed(data)

  def encodeEntryKeyRecord(v: EntryKey.Buffer) = v.toUnframedByteArray
  def decodeEntryKeyRecord(data: Buffer):EntryKey.Buffer = EntryKey.FACTORY.parseUnframed(data)

  def encodeLocator(pos:Long, len:Int):Array[Byte] = {
    val out = new DataByteArrayOutputStream(
      AbstractVarIntSupport.computeVarLongSize(pos)+
      AbstractVarIntSupport.computeVarIntSize(len)
    )
    out.writeVarLong(pos)
    out.writeVarInt(len)
    out.getData
  }
  def decodeLocator(bytes:Buffer):(Long,  Int) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readVarLong(), in.readVarInt())
  }
  def decodeLocator(bytes:Array[Byte]):(Long,  Int) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readVarLong(), in.readVarInt())
  }

  def encodeLongLong(a1:Long, a2:Long) = {
    val out = new DataByteArrayOutputStream(8)
    out.writeLong(a1)
    out.writeLong(a2)
    out.toBuffer
  }

  def decodeLongLong(bytes:Array[Byte]):(Long, Long) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readLong(), in.readLong())
  }

  def encodeLong(a1:Long) = {
    val out = new DataByteArrayOutputStream(8)
    out.writeLong(a1)
    out.toBuffer
  }

  def encodeVLong(a1:Long):Array[Byte] = {
    val out = new DataByteArrayOutputStream(
      AbstractVarIntSupport.computeVarLongSize(a1)
    )
    out.writeVarLong(a1)
    out.getData
  }

  def decodeVLong(bytes:Array[Byte]):Long = {
    val in = new DataByteArrayInputStream(bytes)
    in.readVarLong()
  }

  def encodeLongKey(a1:Byte, a2:Long):Array[Byte] = {
    val out = new DataByteArrayOutputStream(9)
    out.writeByte(a1.toInt)
    out.writeLong(a2)
    out.getData
  }
  def decodeLongKey(bytes:Array[Byte]):(Byte, Long) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readByte(), in.readLong())
  }

  def decodeLong(bytes:Buffer):Long = {
    val in = new DataByteArrayInputStream(bytes)
    in.readLong()
  }
  def decodeLong(bytes:Array[Byte]):Long = {
    val in = new DataByteArrayInputStream(bytes)
    in.readLong()
  }

  def encodeEntryKey(a1:Byte, a2:Long, a3:Long):Array[Byte] = {
    val out = new DataByteArrayOutputStream(17)
    out.writeByte(a1.toInt)
    out.writeLong(a2)
    out.writeLong(a3)
    out.getData
  }

  def encodeEntryKey(a1:Byte, a2:Long, a3:Buffer):Array[Byte] = {
    val out = new DataByteArrayOutputStream(9+a3.length)
    out.writeByte(a1.toInt)
    out.writeLong(a2)
    out.write(a3)
    out.getData
  }
  
  def decodeEntryKey(bytes:Array[Byte]):(Byte, Long, Buffer) = {
    val in = new DataByteArrayInputStream(bytes)
    (in.readByte(), in.readLong(), in.readBuffer(in.available()))
  }

  final class RichDB(val db: DB) {

    val isPureJavaVersion = db.getClass.getName == "org.iq80.leveldb.impl.DbImpl"

    def getProperty(name:String) = db.getProperty(name)

    def getApproximateSizes(ranges:Range*) = db.getApproximateSizes(ranges:_*)

    def get(key:Array[Byte], ro:ReadOptions=new ReadOptions):Option[Array[Byte]] = {
      Option(db.get(key, ro))
    }

    def close:Unit = db.close()

    def delete(key:Array[Byte], wo:WriteOptions=new WriteOptions):Unit = {
      db.delete(key, wo)
    }

    def put(key:Array[Byte], value:Array[Byte], wo:WriteOptions=new WriteOptions):Unit = {
      db.put(key, value, wo)
    }
    
    def write[T](wo:WriteOptions=new WriteOptions, max_write_latency:TimeMetric = TimeMetric())(func: WriteBatch=>T):T = {
      val updates = db.createWriteBatch()
      try {
        val rc=Some(func(updates))
        max_write_latency {
          db.write(updates, wo)
        }
        return rc.get
      } finally {
        updates.close();
      }
    }

    def store[T](write:WriteBatch, wo:WriteOptions=new WriteOptions) = {
      db.write(write, wo)
    }

    def snapshot[T](func: Snapshot=>T):T = {
      val snapshot = db.getSnapshot
      try {
        func(snapshot)
      } finally {
        snapshot.close()
      }
    }

    def cursorKeys(ro:ReadOptions=new ReadOptions)(func: Array[Byte] => Boolean): Unit = {
      val iterator = db.iterator(ro)
      iterator.seekToFirst();
      try {
        while( iterator.hasNext && func(iterator.peekNext.getKey) ) {
          iterator.next()
        }
      } finally {
        iterator.close();
      }
    }

    def cursorKeysPrefixed(prefix:Array[Byte], ro:ReadOptions=new ReadOptions)(func: Array[Byte] => Boolean): Unit = {
      val iterator = db.iterator(ro)
      might_trigger_compaction(iterator.seek(prefix));
      try {
        def check(key:Buffer) = {
          key.startsWith(prefix) && func(key)
        }
        while( iterator.hasNext && check(iterator.peekNext.getKey) ) {
          iterator.next()
        }
      } finally {
        iterator.close();
      }
    }

    def cursorPrefixed(prefix:Array[Byte], ro:ReadOptions=new ReadOptions)(func: (Array[Byte],Array[Byte]) => Boolean): Unit = {
      val iterator = db.iterator(ro)
      might_trigger_compaction(iterator.seek(prefix));
      try {
        def check(key:Buffer) = {
          key.startsWith(prefix) && func(key, iterator.peekNext.getValue)
        }
        while( iterator.hasNext && check(iterator.peekNext.getKey) ) {
          iterator.next()
        }
      } finally {
        iterator.close();
      }
    }

    def compare(a1:Array[Byte], a2:Array[Byte]):Int = {
      new Buffer(a1).compareTo(new Buffer(a2))
    }

    def cursorRangeKeys(startIncluded:Array[Byte], endExcluded:Array[Byte], ro:ReadOptions=new ReadOptions)(func: Array[Byte] => Boolean): Unit = {
      val iterator = db.iterator(ro)
      might_trigger_compaction(iterator.seek(startIncluded));
      try {
        def check(key:Array[Byte]) = {
          if ( compare(key,endExcluded) < 0) {
            func(key)
          } else {
            false
          }
        }
        while( iterator.hasNext && check(iterator.peekNext.getKey) ) {
          iterator.next()
        }
      } finally {
        iterator.close();
      }
    }

    def cursorRange(startIncluded:Array[Byte], endExcluded:Array[Byte], ro:ReadOptions=new ReadOptions)(func: (Array[Byte],Array[Byte]) => Boolean): Unit = {
      val iterator = db.iterator(ro)
      might_trigger_compaction(iterator.seek(startIncluded));
      try {
        def check(key:Array[Byte]) = {
          (compare(key,endExcluded) < 0) && func(key, iterator.peekNext.getValue)
        }
        while( iterator.hasNext && check(iterator.peekNext.getKey) ) {
          iterator.next()
        }
      } finally {
        iterator.close();
      }
    }

    def lastKey(prefix:Array[Byte], ro:ReadOptions=new ReadOptions): Option[Array[Byte]] = {
      val last = new Buffer(prefix).deepCopy().data
      if ( last.length > 0 ) {
        val pos = last.length-1
        last(pos) = (last(pos)+1).toByte
      }

      if(isPureJavaVersion) {
        // The pure java version of LevelDB does not support backward iteration.
        var rc:Option[Array[Byte]] = None
        cursorRangeKeys(prefix, last) { key=>
          rc = Some(key)
          true
        }
        rc
      } else {
        val iterator = db.iterator(ro)
        try {

          might_trigger_compaction(iterator.seek(last));
          if ( iterator.hasPrev ) {
            iterator.prev()
          } else {
            iterator.seekToLast()
          }

          if ( iterator.hasNext ) {
            val key:Buffer = iterator.peekNext.getKey
            if(key.startsWith(prefix)) {
              Some(key)
            } else {
              None
            }
          } else {
            None
          }
        } finally {
          iterator.close();
        }
      }
    }

    def compact = {
      compact_needed = false
      db match {
        case db:JniDB =>
          db.compactRange(null, null)
//        case db:DbImpl =>
//          val start = new Slice(Array[Byte]('a'.toByte))
//          val end = new Slice(Array[Byte]('z'.toByte))
//          db.compactRange(2, start, end)
        case _ =>
      }
    }

    private def might_trigger_compaction[T](func: => T): T = {
      val start = System.nanoTime()
      try {
        func
      } finally {
        val duration = System.nanoTime() - start
        // If it takes longer than 100 ms..
        if( duration > 1000000*100 ) {
          compact_needed = true
        }
      }
    }

    @volatile
    var compact_needed = false
  }


  def bytes(value:String) = value.getBytes("UTF-8")

  import FileSupport._
  def create_sequence_file(directory:File, id:Long, suffix:String) = directory / ("%016x%s".format(id, suffix))

  def find_sequence_files(directory:File, suffix:String):TreeMap[Long, File] = {
    TreeMap((directory.list_files.flatMap { f=>
      if( f.getName.endsWith(suffix) ) {
        try {
          val base = f.getName.stripSuffix(suffix)
          val position = java.lang.Long.parseLong(base, 16);
          Some(position -> f)
        } catch {
          case e:NumberFormatException => None
        }
      } else {
        None
      }
    }): _* )
  }

  class CollectionMeta extends Serializable {
    var size = 0L
    var last_key:Array[Byte] = _
  }

  def copyIndex(from:File, to:File) = {
    for( file <- from.list_files ) {
      val name: String = file.getName
      if( name.endsWith(".sst") ) {
        // SST files don't change once created, safe to hard link.
        file.linkTo(to / name)
      } else if(name == "LOCK")  {
        // No need to copy the lock file.
      } else {
        /// These might not be append only files, so avoid hard linking just to be safe.
        file.copyTo(to / name)
      }
    }
  }
}


/**
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class LevelDBClient(store: LevelDBStore) {

  import LevelDBClient._
  import FileSupport._

  val dispatchQueue = createQueue("leveldb")

  /////////////////////////////////////////////////////////////////////
  //
  // Helpers
  //
  /////////////////////////////////////////////////////////////////////

  def directory = store.directory
  def logDirectory = Option(store.logDirectory).getOrElse(store.directory)

  /////////////////////////////////////////////////////////////////////
  //
  // Public interface used by the DBManager
  //
  /////////////////////////////////////////////////////////////////////

  def sync = store.sync;
  def verifyChecksums = store.verifyChecksums

  var log:RecordLog = _

  var index:RichDB = _
  var plist:RichDB = _
  var indexOptions:Options = _

  var lastIndexSnapshotPos:Long = _
  val snapshotRwLock = new ReentrantReadWriteLock(true)

  var factory:DBFactory = _
  val logRefs = HashMap[Long, LongCounter]()
  var recoveryLogs:java.util.TreeMap[Long, Void] = _

  val collectionMeta = HashMap[Long, CollectionMeta]()

  def plistIndexFile = directory / ("plist"+INDEX_SUFFIX)
  def dirtyIndexFile = directory / ("dirty"+INDEX_SUFFIX)
  def tempIndexFile = directory / ("temp"+INDEX_SUFFIX)
  def snapshotIndexFile(id:Long) = create_sequence_file(directory,id, INDEX_SUFFIX)

  def size: Long = logRefs.size * store.logSize

  def createLog: RecordLog = {
    new RecordLog(logDirectory, LOG_SUFFIX)
  }

  var writeExecutor:ExecutorService = _

  def writeExecutorExec(func: =>Unit ) = writeExecutor {
    func
  }

  def storeTrace(ascii:String, force:Boolean=false) = {
    assert_write_thread_executing
    val time = new SimpleDateFormat("dd/MMM/yyyy:HH:mm::ss Z").format(new Date)
    log.appender { appender =>
      appender.append(LOG_TRACE, new AsciiBuffer("%s: %s".format(time, ascii)))
      if( force ) {
        appender.force
      }
    }
  }

  def might_fail[T](func : =>T):T = {
    def handleFailure(e:IOException) = {
      var failure:Throwable = e;
      if( store.broker_service !=null ) {
        // This should start stopping the broker but it might block,
        // so do it on another thread...
        new Thread("LevelDB IOException handler.") {
          override def run() {
            try {
              store.broker_service.handleIOException(e)
            } catch {
              case e:RuntimeException =>
                failure = e
            } finally {
              store.stop()
            }
          }
        }.start()
        // Lets wait until the broker service has started stopping.  Once the
        // stopping flag is raised, errors caused by stopping the store should
        // not get propagated to the client.
        while( !store.broker_service.isStopping ) {
          Thread.sleep(100);
        }
      }
      throw new SuppressReplyException(failure);
    }
    try {
      func
    } catch {
      case e:IOException => handleFailure(e)
      case e:Throwable => handleFailure(IOExceptionSupport.create(e))
    }
  }

  def start() = {
    init()
    replay_init()
    might_fail {
      log.open()
    }

    var startPosition = lastIndexSnapshotPos;
    // if we cannot locate a log for a snapshot, replay from
    // first entry of first available log
    if (log.log_info(startPosition).isEmpty) {
        if (!log.log_infos.isEmpty) {
          startPosition = log.log_infos.firstKey();
        }
    }

    replay_from(startPosition, log.appender_limit)
    replay_write_batch = null;
  }

  def assert_write_thread_executing = assert(Thread.currentThread().getClass == classOf[WriteThread])

  def init() ={

    // Lets check store compatibility...
    directory.mkdirs()
    val version_file = directory / "store-version.txt"
    if (version_file.exists()) {
      val ver = try {
        var tmp: String = version_file.readText().trim()
        if (tmp.startsWith(STORE_SCHEMA_PREFIX)) {
          tmp.stripPrefix(STORE_SCHEMA_PREFIX).toInt
        } else {
          -1
        }
      } catch {
        case e:Throwable => throw new Exception("Unexpected version file format: " + version_file)
      }
      ver match {
        case STORE_SCHEMA_VERSION => // All is good.
        case _ => throw new Exception("Cannot open the store.  It's schema version is not supported.")
      }
    }
    version_file.writeText(STORE_SCHEMA_PREFIX + STORE_SCHEMA_VERSION)

    writeExecutor = Executors.newFixedThreadPool(1, new ThreadFactory() {
      def newThread(r: Runnable) = new WriteThread(r)
    })

    val factoryNames = store.indexFactory
    factory = factoryNames.split("""(,|\s)+""").map(_.trim()).flatMap { name=>
      try {
        Some(this.getClass.getClassLoader.loadClass(name).newInstance().asInstanceOf[DBFactory])
      } catch {
        case e:Throwable =>
          debug("Could not load factory: "+name+" due to: "+e)
          None
      }
    }.headOption.getOrElse(throw new Exception("Could not load any of the index factory classes: "+factoryNames))

    if( factory.getClass.getName == "org.iq80.leveldb.impl.Iq80DBFactory") {
      info("Using the pure java LevelDB implementation.")
    }
    if( factory.getClass.getName == "org.fusesource.leveldbjni.JniDBFactory") {
      info("Using the JNI LevelDB implementation.")
    }

    indexOptions = new Options();
    indexOptions.createIfMissing(true);

    indexOptions.maxOpenFiles(store.indexMaxOpenFiles)
    indexOptions.blockRestartInterval(store.indexBlockRestartInterval)
    indexOptions.paranoidChecks(store.paranoidChecks)
    indexOptions.writeBufferSize(store.indexWriteBufferSize)
    indexOptions.blockSize(store.indexBlockSize)
    indexOptions.compressionType( store.indexCompression.toLowerCase match {
      case "snappy" => CompressionType.SNAPPY
      case "none" => CompressionType.NONE
      case _ => CompressionType.SNAPPY
    })

    indexOptions.cacheSize(store.indexCacheSize)
    indexOptions.logger(new Logger() {
      val LOG = Log(factory.getClass.getName)
      def log(msg: String) = LOG.debug("index: "+msg.stripSuffix("\n"))
    })

    log = createLog
    log.logSize = store.logSize
    log.on_log_rotate = ()=> {
      post_log_rotate
    }
  }

  def post_log_rotate ={
      // We snapshot the index every time we rotate the logs.
      writeExecutor {
        snapshotIndex(false)
      }
  }

  def replay_init() = {
    // Find out what was the last snapshot.
    val snapshots = find_sequence_files(directory, INDEX_SUFFIX)
    var lastSnapshotIndex = snapshots.lastOption
    lastIndexSnapshotPos = lastSnapshotIndex.map(_._1).getOrElse(0)

    // Only keep the last snapshot..
    snapshots.filterNot(_._1 == lastIndexSnapshotPos).foreach( _._2.recursiveDelete )
    tempIndexFile.recursiveDelete

    might_fail {
      // Setup the plist index.
      plistIndexFile.recursiveDelete
      plistIndexFile.mkdirs()
      plist = new RichDB(factory.open(plistIndexFile, indexOptions));

      // Delete the dirty indexes
      dirtyIndexFile.recursiveDelete
      dirtyIndexFile.mkdirs()

      for( (id, file)<- lastSnapshotIndex ) {
        try {
          copyIndex(file, dirtyIndexFile)
          debug("Recovering from last index snapshot at: "+dirtyIndexFile)
        } catch {
          case e:Exception =>
            warn(e, "Could not recover snapshot of the index: "+e)
            lastSnapshotIndex  = None
        }
      }
      index = new RichDB(factory.open(dirtyIndexFile, indexOptions));
      for(value <- index.get(DIRTY_INDEX_KEY) ) {
        if( java.util.Arrays.equals(value, TRUE) ) {
          warn("Recovering from a dirty index.")
        }
      }
      index.put(DIRTY_INDEX_KEY, TRUE)
      loadCounters
    }
  }

  var replay_write_batch: WriteBatch = null
  var indexRecoveryPosition = 0L

  def replay_from(from:Long, limit:Long, print_progress:Boolean=true) = {
    debug("Replay of journal from: %d to %d.", from, limit)
    if( replay_write_batch==null ) {
      replay_write_batch = index.db.createWriteBatch()
    }
    might_fail {
      try {
        // Update the index /w what was stored on the logs..
        indexRecoveryPosition = from;
        var last_reported_at = System.currentTimeMillis();
        var showing_progress = false
        var last_reported_pos = 0L
        try {
          while (indexRecoveryPosition < limit) {

            if( print_progress ) {
              val now = System.currentTimeMillis();
              if( now > last_reported_at+1000 ) {
                val at = indexRecoveryPosition-from
                val total = limit-from
                val rate = (indexRecoveryPosition-last_reported_pos)*1000.0 / (now - last_reported_at)
                val eta = (total-at)/rate
                val remaining = if(eta > 60*60) {
                  "%.2f hrs".format(eta/(60*60))
                } else if(eta > 60) {
                  "%.2f mins".format(eta/60)
                } else {
                  "%.0f secs".format(eta)
                }

                System.out.print("Replaying recovery log: %f%% done (%,d/%,d bytes) @ %,.2f kb/s, %s remaining.     \r".format(
                  at*100.0/total, at, total, rate/1024, remaining))
                showing_progress = true;
                last_reported_at = now
                last_reported_pos = indexRecoveryPosition
              }
            }


            log.read(indexRecoveryPosition).map {
              case (kind, data, nextPos) =>
                kind match {
                  case LOG_DATA =>
                    val message = decodeMessage(data)
                    store.db.producerSequenceIdTracker.isDuplicate(message.getMessageId)
                    trace("Replay of LOG_DATA at %d, message id: ", indexRecoveryPosition, message.getMessageId)

                  case LOG_ADD_COLLECTION =>
                    val record= decodeCollectionRecord(data)
                    replay_write_batch.put(encodeLongKey(COLLECTION_PREFIX, record.getKey), data)
                    collectionMeta.put(record.getKey, new CollectionMeta)
                    trace("Replay of LOG_ADD_COLLECTION at %d, collection: %s", indexRecoveryPosition, record.getKey)

                  case LOG_REMOVE_COLLECTION =>
                    val record = decodeCollectionKeyRecord(data)
                    // Delete the entries in the collection.
                    index.cursorPrefixed(encodeLongKey(ENTRY_PREFIX, record.getKey), new ReadOptions) { (key, value)=>
                      val record = decodeEntryRecord(value)
                      val pos = if ( record.hasValueLocation ) {
                        Some(record.getValueLocation)
                      } else {
                        None
                      }
                      pos.foreach(logRefDecrement(_))
                      index.delete(key)
                      true
                    }
                    index.delete(data)
                    collectionMeta.remove(record.getKey)
                    trace("Replay of LOG_REMOVE_COLLECTION at %d, collection: %s", indexRecoveryPosition, record.getKey)

                  case LOG_ADD_ENTRY | LOG_UPDATE_ENTRY =>
                    val record = decodeEntryRecord(data)

                    val index_record = new EntryRecord.Bean()
                    index_record.setValueLocation(record.getValueLocation)
                    if( record.hasValueLength ) {
                      index_record.setValueLength(record.getValueLength)
                    }
                    val index_value = encodeEntryRecord(index_record.freeze()).toByteArray

                    replay_write_batch.put(encodeEntryKey(ENTRY_PREFIX, record.getCollectionKey, record.getEntryKey), index_value)

                    if( kind==LOG_ADD_ENTRY ) {
                      logRefIncrement(record.getValueLocation)
                      collectionIncrementSize(record.getCollectionKey, record.getEntryKey.toByteArray)
                      trace("Replay of LOG_ADD_ENTRY at %d, collection: %s, entry: %s", indexRecoveryPosition, record.getCollectionKey, record.getEntryKey)
                    } else {
                      trace("Replay of LOG_UPDATE_ENTRY at %d, collection: %s, entry: %s", indexRecoveryPosition, record.getCollectionKey, record.getEntryKey)
                    }

                  case LOG_REMOVE_ENTRY =>
                    val record = decodeEntryRecord(data)

                    // Figure out which log file this message reference is pointing at..
                    if ( record.hasValueLocation ) {
                      logRefDecrement(record.getValueLocation)
                    }

                    replay_write_batch.delete(encodeEntryKey(ENTRY_PREFIX, record.getCollectionKey, record.getEntryKey))
                    collectionDecrementSize( record.getCollectionKey)
                    trace("Replay of LOG_REMOVE_ENTRY collection: %s, entry: %s", indexRecoveryPosition, record.getCollectionKey, record.getEntryKey)

                  case LOG_TRACE =>
                    trace("Replay of LOG_TRACE, message: %s", indexRecoveryPosition, data.ascii())
                  case RecordLog.UOW_END_RECORD =>
                    trace("Replay of UOW_END_RECORD")
                    index.db.write(replay_write_batch)
                    replay_write_batch=index.db.createWriteBatch()
                  case kind => // Skip other records, they don't modify the index.
                    trace("Skipping replay of %d record kind at %d", kind, indexRecoveryPosition)

                }
                indexRecoveryPosition = nextPos
            }
          }
        }
        catch {
          case e:Throwable => e.printStackTrace()
        }
        if(showing_progress) {
          System.out.println("Replaying recovery log: 100% done                                 ");
        }

      } catch {
        case e:Throwable =>
          // replay failed.. good thing we are in a retry block...
          index.close
          replay_write_batch = null
          throw e;
      } finally {
        recoveryLogs = null
        debug("Replay end")
      }
    }
  }

  private def logRefDecrement(pos: Long) {
    for( key <- logRefKey(pos) ) {
      logRefs.get(key) match {
        case Some(counter) => counter.decrementAndGet() == 0
        case None => warn("invalid: logRefDecrement: "+pos)
      }
    }
  }

  private def logRefIncrement(pos: Long) {
    for( key <- logRefKey(pos) ) {
      logRefs.getOrElseUpdate(key, new LongCounter(0)).incrementAndGet()
    }
  }

  def logRefKey(pos: Long, log_info: RecordLog.LogInfo=null): Option[Long] = {
    if( log_info!=null ) {
      Some(log_info.position)
    } else {
      val rc = if( recoveryLogs !=null ) {
        Option(recoveryLogs.floorKey(pos))
      } else {
        log.log_info(pos).map(_.position)
      }
      if( !rc.isDefined ) {
        warn("Invalid log position: " + pos)
      }
      rc
    }
  }

  private def collectionDecrementSize(key: Long) {
    collectionMeta.get(key).foreach(_.size -= 1)
  }
  private def collectionIncrementSize(key: Long, last_key:Array[Byte]) {
    collectionMeta.get(key).foreach{ x=> 
      x.size += 1
      x.last_key = last_key
    }
  }

  private def storeCounters = {
    def storeMap[T <: AnyRef](key:Array[Byte], map:HashMap[Long, T]) {
      val baos = new ByteArrayOutputStream()
      val os = new ObjectOutputStream(baos);
      os.writeInt(map.size);
      for( (k,v) <- map ) {
        os.writeLong(k)
        os.writeObject(v)
      }
      os.close()
      try {
        index.put(key, baos.toByteArray)
      }
      catch {
        case e : Throwable => throw e
      }
    }
    def storeList[T <: AnyRef](key:Array[Byte], list:Array[Long]) {
      val baos = new ByteArrayOutputStream()
      val os = new ObjectOutputStream(baos);
      os.writeInt(list.size);
      for( k <- list ) {
        os.writeLong(k)
      }
      os.close()
      try {
        index.put(key, baos.toByteArray)
      }
      catch {
        case e : Throwable => throw e
      }
    }
    def storeObject(key:Array[Byte], o:Object) = {
      val baos = new ByteArrayOutputStream()
      val os = new ObjectOutputStream(baos);
      os.writeObject(o)
      os.close()
      index.put(key, baos.toByteArray)
    }

    storeMap(LOG_REF_INDEX_KEY, logRefs)
    storeMap(COLLECTION_META_KEY, collectionMeta)
    storeList(LOGS_INDEX_KEY, log.log_file_positions)
    storeObject(PRODUCER_IDS_INDEX_KEY, store.db.producerSequenceIdTracker)

  }

  private def loadCounters = {
    def loadMap[T <: AnyRef](key:Array[Byte], map:HashMap[Long, T]) {
      map.clear()
      index.get(key, new ReadOptions).foreach { value=>
        val bais = new ByteArrayInputStream(value)
        val is = new ObjectInputStream(bais);
        var remaining = is.readInt()
        while(remaining > 0 ) {
          map.put(is.readLong(), is.readObject().asInstanceOf[T])
          remaining-=1
        }
      }
    }
    def loadList[T <: AnyRef](key:Array[Byte]) = {
      index.get(key, new ReadOptions).map { value=>
        val rc = ListBuffer[Long]()
        val bais = new ByteArrayInputStream(value)
        val is = new ObjectInputStream(bais);
        var remaining = is.readInt()
        while(remaining > 0 ) {
          rc.append(is.readLong())
          remaining-=1
        }
        rc
      }
    }
    def loadObject(key:Array[Byte]) = {
      index.get(key, new ReadOptions).map { value=>
        val bais = new ByteArrayInputStream(value)
        val is = new ObjectInputStream(bais);
        is.readObject();
      }
    }

    loadMap(LOG_REF_INDEX_KEY, logRefs)
    loadMap(COLLECTION_META_KEY, collectionMeta)
    for( list <- loadList(LOGS_INDEX_KEY) ) {
      recoveryLogs = new java.util.TreeMap[Long, Void]()
      for( k <- list ) {
        recoveryLogs.put(k, null)
      }
    }
    for( audit <- loadObject(PRODUCER_IDS_INDEX_KEY) ) {
      store.db.producerSequenceIdTracker = audit.asInstanceOf[ActiveMQMessageAuditNoSync]
    }
  }


  var stored_wal_append_position = 0L

  def wal_append_position = this.synchronized {
    if (log!=null && log.isOpen) {
      log.appender_limit
    } else {
      stored_wal_append_position
    }
  }

  def dirty_stop = this.synchronized {
    def ingorefailure(func: =>Unit) = try { func } catch { case e:Throwable=> }
    ingorefailure(index.close)
    ingorefailure(log.close)
    ingorefailure(plist.close)
    ingorefailure(might_fail(throw new IOException("non-clean close")))
  }

  def stop():Unit = {
    var executorToShutdown:ExecutorService = null
    this synchronized {
      if (writeExecutor != null) {
        executorToShutdown = writeExecutor
        writeExecutor = null
      }
    }

    if (executorToShutdown != null) {
      executorToShutdown.shutdown
      executorToShutdown.awaitTermination(60, TimeUnit.SECONDS)

      // this blocks until all io completes..
      snapshotRwLock.writeLock().lock()
      try {
        // Suspend also deletes the index.
        if( index!=null ) {
          storeCounters
          index.put(DIRTY_INDEX_KEY, FALSE, new WriteOptions().sync(true))
          index.close
          index = null
          debug("Gracefuly closed the index")
          copyDirtyIndexToSnapshot
        }
        this synchronized {
          if (log!=null && log.isOpen) {
            log.close
            stored_wal_append_position = log.appender_limit
            log = null
          }
        }
        if( plist!=null ) {
          plist.close
          plist=null
        }
      } finally {
        snapshotRwLock.writeLock().unlock()
      }
    }
  }

  def usingIndex[T](func: =>T):T = {
    val lock = snapshotRwLock.readLock();
    lock.lock()
    try {
      func
    } finally {
      lock.unlock()
    }
  }

  def might_fail_using_index[T](func: =>T):T = might_fail(usingIndex( func ))

  /**
   * TODO: expose this via management APIs, handy if you want to
   * do a file system level snapshot and want the data to be consistent.
   */
  def suspend() = {
    // Make sure we are the only ones accessing the index. since
    // we will be closing it to create a consistent snapshot.
    snapshotRwLock.writeLock().lock()

    storeCounters
    index.put(DIRTY_INDEX_KEY, FALSE, new WriteOptions().sync(true))
    // Suspend the index so that it's files are not changed async on us.
    index.db.suspendCompactions()
  }

  /**
   * TODO: expose this via management APIs, handy if you want to
   * do a file system level snapshot and want the data to be consistent.
   */
  def resume() = {
    // re=open it..
    index.db.resumeCompactions()
    snapshotRwLock.writeLock().unlock()
  }

  def nextIndexSnapshotPos:Long = wal_append_position

  def copyDirtyIndexToSnapshot:Unit = {
    if( nextIndexSnapshotPos == lastIndexSnapshotPos  ) {
      // no need to snapshot again...
      return
    }
    copyDirtyIndexToSnapshot(nextIndexSnapshotPos)
  }

  def copyDirtyIndexToSnapshot(walPosition:Long):Unit = {
    debug("Taking a snapshot of the current index: "+snapshotIndexFile(walPosition))
    // Where we start copying files into.  Delete this on
    // restart.
    val tmpDir = tempIndexFile
    tmpDir.mkdirs()

    try {

      // Copy the index to the tmp dir.
      copyIndex(dirtyIndexFile, tmpDir)

      // Rename to signal that the snapshot is complete.
      tmpDir.renameTo(snapshotIndexFile(walPosition))
      replaceLatestSnapshotDirectory(walPosition)

    } catch {
      case e: Exception =>
        // if we could not snapshot for any reason, delete it as we don't
        // want a partial check point..
        warn(e, "Could not snapshot the index: " + e)
        tmpDir.recursiveDelete
    }
  }

  def replaceLatestSnapshotDirectory(newSnapshotIndexPos: Long) {
    snapshotIndexFile(lastIndexSnapshotPos).recursiveDelete
    lastIndexSnapshotPos = newSnapshotIndexPos
  }

  def snapshotIndex(sync:Boolean=false):Unit = {
    suspend()
    try {
      if( sync ) {
        log.current_appender.force
      }
      copyDirtyIndexToSnapshot
    } finally {
      resume()
    }
  }

  def purge() = {
    suspend()
    try{
      log.close
      locked_purge
    } finally {
      might_fail {
        log.open()
      }
      resume()
    }
  }

  def locked_purge {
    for( x <- logDirectory.list_files) {
      if (x.getName.endsWith(".log")) {
        x.delete()
      }
    }
    for( x <- directory.list_files) {
      if (x.getName.endsWith(".index")) {
        x.recursiveDelete
      }
    }
  }

  def addCollection(record: CollectionRecord.Buffer) = {
    assert_write_thread_executing

    val key = encodeLongKey(COLLECTION_PREFIX, record.getKey)
    val value = record.toUnframedBuffer
    might_fail_using_index {
      log.appender { appender =>
        appender.append(LOG_ADD_COLLECTION, value)
        index.put(key, value.toByteArray)
      }
    }
    collectionMeta.put(record.getKey, new CollectionMeta)
  }

  def getLogAppendPosition = log.appender_limit

  def listCollections: Seq[(Long, CollectionRecord.Buffer)] = {
    val rc = ListBuffer[(Long, CollectionRecord.Buffer)]()
    might_fail_using_index {
      val ro = new ReadOptions
      ro.verifyChecksums(verifyChecksums)
      ro.fillCache(false)
      index.cursorPrefixed(COLLECTION_PREFIX_ARRAY, ro) { (key, value) =>
        rc.append(( decodeLongKey(key)._2, CollectionRecord.FACTORY.parseUnframed(value) ))
        true // to continue cursoring.
      }
    }
    rc
  }

  def removeCollection(collectionKey: Long) = {
    assert_write_thread_executing
    val key = encodeLongKey(COLLECTION_PREFIX, collectionKey)
    val value = encodeVLong(collectionKey)
    val entryKeyPrefix = encodeLongKey(ENTRY_PREFIX, collectionKey)
    collectionMeta.remove(collectionKey)
    might_fail_using_index {
      log.appender { appender =>
        appender.append(LOG_REMOVE_COLLECTION, new Buffer(value))
      }

      val ro = new ReadOptions
      ro.fillCache(false)
      ro.verifyChecksums(verifyChecksums)
      index.cursorPrefixed(entryKeyPrefix, ro) { (key, value)=>
        val record = decodeEntryRecord(value)
        val pos = if ( record.hasValueLocation ) {
          Some(record.getValueLocation)
        } else {
          None
        }
        pos.foreach(logRefDecrement(_))
        index.delete(key)
        true
      }
      index.delete(key)
    }
  }

  def collectionEmpty(collectionKey: Long) = {
    assert_write_thread_executing
    val key = encodeLongKey(COLLECTION_PREFIX, collectionKey)
    val value = encodeVLong(collectionKey)
    val entryKeyPrefix = encodeLongKey(ENTRY_PREFIX, collectionKey)

    val meta = collectionMeta.getOrElseUpdate(collectionKey, new CollectionMeta)
    meta.size = 0
    meta.last_key = null
    
    might_fail_using_index {
      index.get(key).foreach { collectionData =>
        log.appender { appender =>
          appender.append(LOG_REMOVE_COLLECTION, new Buffer(value))
          appender.append(LOG_ADD_COLLECTION, new Buffer(collectionData))
        }

        val ro = new ReadOptions
        ro.fillCache(false)
        ro.verifyChecksums(verifyChecksums)
        index.cursorPrefixed(entryKeyPrefix, ro) { (key, value)=>
          val record = decodeEntryRecord(value)
          val pos = if ( record.hasValueLocation ) {
            Some(record.getValueLocation)
          } else {
            None
          }
          pos.foreach(logRefDecrement(_))
          index.delete(key)
          true
        }
      }
    }
  }

  def decodeQueueEntryMeta(value:EntryRecord.Getter):Int= {
    if( value.hasMeta ) {
      val is = new DataByteArrayInputStream(value.getMeta);
      val metaVersion = is.readVarInt()
      metaVersion match {
        case 1 =>
          return is.readVarInt()
        case _ =>
      }
    }
    return 0
  }

  def getDeliveryCounter(collectionKey: Long, seq:Long):Int = {
    val ro = new ReadOptions
    ro.fillCache(true)
    ro.verifyChecksums(verifyChecksums)
    val key = encodeEntryKey(ENTRY_PREFIX, collectionKey, encodeLong(seq))
    var rc = 0
    might_fail_using_index {
      for( v <- index.get(key, ro) ) {
        rc = decodeQueueEntryMeta(EntryRecord.FACTORY.parseUnframed(v))
      }
    }
    return rc
  }

  def queueCursor(collectionKey: Long, seq:Long, endSeq:Long)(func: (Message)=>Boolean) = {
    collectionCursor(collectionKey, encodeLong(seq), encodeLong(endSeq)) { (key, value) =>
      val seq = decodeLong(key)
      var locator = DataLocator(store, value.getValueLocation, value.getValueLength)
      val msg = getMessage(locator)
      if( msg !=null ) {
        msg.getMessageId().setEntryLocator(EntryLocator(collectionKey, seq))
        msg.getMessageId().setDataLocator(locator)
        msg.setRedeliveryCounter(decodeQueueEntryMeta(value))
        func(msg)
      } else {
        warn("Could not load message seq: "+seq+" from "+locator)
        true
      }
    }
  }

  def transactionCursor(collectionKey: Long)(func: (AnyRef)=>Boolean) = {
    collectionCursor(collectionKey, encodeLong(0), encodeLong(Long.MaxValue)) { (key, value) =>
      val seq = decodeLong(key)
      if( value.getMeta != null ) {

        val is = new DataByteArrayInputStream(value.getMeta);
        val log = is.readLong()
        val offset = is.readInt()
        val qid = is.readLong()
        val seq = is.readLong()
        val sub = is.readLong()
        val ack = store.wireFormat.unmarshal(is).asInstanceOf[MessageAck]
        ack.getLastMessageId.setDataLocator(DataLocator(store, log, offset))
        ack.getLastMessageId.setEntryLocator(EntryLocator(qid, seq))

        func(XaAckRecord(collectionKey, seq, ack, sub))
      } else {
        var locator = DataLocator(store, value.getValueLocation, value.getValueLength)
        val msg = getMessage(locator)
        if( msg !=null ) {
          msg.getMessageId().setEntryLocator(EntryLocator(collectionKey, seq))
          msg.getMessageId().setDataLocator(locator)
          func(msg)
        } else {
          warn("Could not load XA message seq: "+seq+" from "+locator)
          true
        }
      }
    }
  }

  def getAckPosition(subKey: Long): Long = {
    might_fail_using_index {
      index.get(encodeEntryKey(ENTRY_PREFIX, subKey, ACK_POSITION)).map{ value=>
        val record = decodeEntryRecord(value)
        record.getValueLocation()
      }.getOrElse(0L)
    }
  }

  def getMessage(locator:AnyRef):Message = {
    assert(locator!=null)
    val buffer = locator match {
      case x:MessageRecord =>
        // Encoded form is still in memory..
        Some(x.data)
      case DataLocator(store, pos, len) =>
        // Load the encoded form from disk.
        log.read(pos, len).map(new Buffer(_))
    }

    // Lets decode
    buffer.map(decodeMessage(_)).getOrElse(null)
  }

  def decodeMessage(x: Buffer): Message = {
    var data = if (store.snappyCompressLogs) {
      Snappy.uncompress(x)
    } else {
      x
    }
    store.wireFormat.unmarshal(new ByteSequence(data.data, data.offset, data.length)).asInstanceOf[Message]
  }

  def collectionCursor(collectionKey: Long, cursorPosition:Buffer, endCursorPosition:Buffer)(func: (Buffer, EntryRecord.Buffer)=>Boolean) = {
    val ro = new ReadOptions
    ro.fillCache(true)
    ro.verifyChecksums(verifyChecksums)
    val start = encodeEntryKey(ENTRY_PREFIX, collectionKey, cursorPosition)
    val end = encodeEntryKey(ENTRY_PREFIX, collectionKey, endCursorPosition)
    might_fail_using_index {
      index.cursorRange(start, end, ro) { case (key, value) =>
        func(key.buffer.moveHead(9), EntryRecord.FACTORY.parseUnframed(value))
      }
    }
  }

  def collectionSize(collectionKey: Long) = {
    collectionMeta.get(collectionKey).map(_.size).getOrElse(0L)
  }

  def collectionIsEmpty(collectionKey: Long) = {
    val entryKeyPrefix = encodeLongKey(ENTRY_PREFIX, collectionKey)
    var empty = true
    might_fail_using_index {
      val ro = new ReadOptions
      ro.fillCache(false)
      ro.verifyChecksums(verifyChecksums)
      index.cursorKeysPrefixed(entryKeyPrefix, ro) { key =>
        empty = false
        false
      }
    }
    empty
  }

  val max_write_message_latency = TimeMetric()
  val max_write_enqueue_latency = TimeMetric()

  val max_index_write_latency = TimeMetric()

  def store(uows: Array[DelayableUOW]) {
    assert_write_thread_executing
    might_fail_using_index {
      log.appender { appender =>
        val syncNeeded = index.write(new WriteOptions, max_index_write_latency) { batch =>
          write_uows(uows, appender, batch)
        }
        if( syncNeeded && sync ) {
          appender.force
        }
      } // end of log.appender { block }

      // now that data is logged.. locate message from the data in the logs
      for( uow <- uows ) {
        for((msg, action) <- uow.actions ){
          val messageRecord = action.messageRecord
          if (messageRecord != null) {
            messageRecord.id.setDataLocator(messageRecord.locator)
          }
        }
      }
    }
  }


  def write_uows(uows: Array[DelayableUOW], appender: RecordLog#LogAppender, batch: WriteBatch) = {
    var syncNeeded = false
    var write_message_total = 0L
    var write_enqueue_total = 0L

    for( uow <- uows ) {
      for( (msg, action) <- uow.actions ) {
        val messageRecord = action.messageRecord
        var log_info: LogInfo = null
        var dataLocator: DataLocator = null

        if (messageRecord != null && messageRecord.locator == null) {
          store.db.producerSequenceIdTracker.isDuplicate(messageRecord.id)
          val start = System.nanoTime()
          val p = appender.append(LOG_DATA, messageRecord.data)
          log_info = p._2
          dataLocator = DataLocator(store, p._1, messageRecord.data.length)
          messageRecord.locator = dataLocator
//          println("msg: "+messageRecord.id+" -> "+dataLocator)
          write_message_total += System.nanoTime() - start
        }


        for( entry <- action.dequeues) {
          val keyLocation = entry.id.getEntryLocator.asInstanceOf[EntryLocator]
          val key = encodeEntryKey(ENTRY_PREFIX, keyLocation.qid, keyLocation.seq)

          if (dataLocator == null) {
            dataLocator = entry.id.getDataLocator match {
              case x: DataLocator => x
              case x: MessageRecord => x.locator
              case _ => throw new RuntimeException("Unexpected locator type: " + dataLocator)
            }
          }

//          println("deq: "+entry.id+" -> "+dataLocator)
          val log_record = new EntryRecord.Bean()
          log_record.setCollectionKey(entry.queueKey)
          log_record.setEntryKey(new Buffer(key, 9, 8))
          log_record.setValueLocation(dataLocator.pos)
          appender.append(LOG_REMOVE_ENTRY, encodeEntryRecord(log_record.freeze()))

          batch.delete(key)
          logRefDecrement(dataLocator.pos)
          collectionDecrementSize(entry.queueKey)
        }

        for( entry<- action.enqueues) {

          if (dataLocator == null) {
            dataLocator = entry.id.getDataLocator match {
              case x: DataLocator => x
              case x: MessageRecord => x.locator
              case _ =>
                throw new RuntimeException("Unexpected locator type")
            }
          }

//          println("enq: "+entry.id+" -> "+dataLocator)
          val start = System.nanoTime()

          val key = encodeEntryKey(ENTRY_PREFIX, entry.queueKey, entry.queueSeq)

          assert(entry.id.getDataLocator() != null)

          val log_record = new EntryRecord.Bean()
          log_record.setCollectionKey(entry.queueKey)
          log_record.setEntryKey(new Buffer(key, 9, 8))
          log_record.setValueLocation(dataLocator.pos)
          log_record.setValueLength(dataLocator.len)

          val kind = if (entry.deliveries==0) LOG_ADD_ENTRY else LOG_UPDATE_ENTRY
          appender.append(kind, encodeEntryRecord(log_record.freeze()))

          val index_record = new EntryRecord.Bean()
          index_record.setValueLocation(dataLocator.pos)
          index_record.setValueLength(dataLocator.len)

          // Store the delivery counter.
          if( entry.deliveries!=0 ) {
            val os = new DataByteArrayOutputStream()
            os.writeVarInt(1) // meta data format version
            os.writeVarInt(entry.deliveries)
            index_record.setMeta(os.toBuffer)
          }

          val index_data = encodeEntryRecord(index_record.freeze()).toByteArray
          batch.put(key, index_data)

          if( kind==LOG_ADD_ENTRY ) {
            logRefIncrement(dataLocator.pos)
            collectionIncrementSize(entry.queueKey, log_record.getEntryKey.toByteArray)
          }

          write_enqueue_total += System.nanoTime() - start
        }

        for( entry <- action.xaAcks ) {

          val ack = entry.ack
          if (dataLocator == null) {
            dataLocator = ack.getLastMessageId.getDataLocator match {
              case x: DataLocator => x
              case x: MessageRecord => x.locator
              case _ =>
                throw new RuntimeException("Unexpected locator type")
            }
          }
//          println(dataLocator)

          val el = ack.getLastMessageId.getEntryLocator.asInstanceOf[EntryLocator];
          val os = new DataByteArrayOutputStream()
          os.writeLong(dataLocator.pos)
          os.writeInt(dataLocator.len)
          os.writeLong(el.qid)
          os.writeLong(el.seq)
          os.writeLong(entry.sub)
          store.wireFormat.marshal(ack, os)
          var ack_encoded = os.toBuffer

          val key = encodeEntryKey(ENTRY_PREFIX, entry.container, entry.seq)
          val log_record = new EntryRecord.Bean()
          log_record.setCollectionKey(entry.container)
          log_record.setEntryKey(new Buffer(key, 9, 8))
          log_record.setMeta(ack_encoded)
          appender.append(LOG_ADD_ENTRY, encodeEntryRecord(log_record.freeze()))
          val index_record = new EntryRecord.Bean()
          index_record.setMeta(ack_encoded)
          batch.put(key, encodeEntryRecord(log_record.freeze()).toByteArray)
        }
      }

      for( entry <- uow.subAcks ) {
        val key = encodeEntryKey(ENTRY_PREFIX, entry.subKey, ACK_POSITION)
        val log_record = new EntryRecord.Bean()
        log_record.setCollectionKey(entry.subKey)
        log_record.setEntryKey(ACK_POSITION)
        log_record.setValueLocation(entry.ackPosition)
        appender.append(LOG_UPDATE_ENTRY, encodeEntryRecord(log_record.freeze()))

        val index_record = new EntryRecord.Bean()
        index_record.setValueLocation(entry.ackPosition)
        batch.put(key, encodeEntryRecord(index_record.freeze()).toByteArray)
      }

      if (uow.syncNeeded) {
        syncNeeded = true
      }
    }

    max_write_message_latency.add(write_message_total)
    max_write_enqueue_latency.add(write_enqueue_total)
    syncNeeded
  }

  def getCollectionEntries(collectionKey: Long, firstSeq:Long, lastSeq:Long): Seq[(Buffer, EntryRecord.Buffer)] = {
    var rc = ListBuffer[(Buffer, EntryRecord.Buffer)]()
    val ro = new ReadOptions
    ro.verifyChecksums(verifyChecksums)
    ro.fillCache(true)
    might_fail_using_index {
      index.snapshot { snapshot =>
        ro.snapshot(snapshot)
        val start = encodeEntryKey(ENTRY_PREFIX, collectionKey, firstSeq)
        val end = encodeEntryKey(ENTRY_PREFIX, collectionKey, lastSeq+1)
        index.cursorRange( start, end, ro ) { (key, value) =>
          val (_, _, seq) = decodeEntryKey(key)
          rc.append((seq, EntryRecord.FACTORY.parseUnframed(value)))
          true
        }
      }
    }
    rc
  }

  def getLastQueueEntrySeq(collectionKey: Long): Long = {
    getLastCollectionEntryKey(collectionKey).map(_.bigEndianEditor().readLong()).getOrElse(0L)
  }

  def getLastCollectionEntryKey(collectionKey: Long): Option[Buffer] = {
    collectionMeta.get(collectionKey).flatMap(x=> Option(x.last_key)).map(new Buffer(_))
  }

  // APLO-245: lets try to detect when leveldb needs a compaction..
  private def detect_if_compact_needed:Unit = {

    // auto compaction might be disabled...
    if ( store.autoCompactionRatio <= 0 ) {
      return
    }

    // How much space is the dirty index using??
    var index_usage = 0L
    for( file <- dirtyIndexFile.recursiveList ) {
      if(!file.isDirectory && file.getName.endsWith(".sst") ) {
        index_usage += file.length()
      }
    }

    // Lets use the log_refs to get a rough estimate on how many entries are store in leveldb.
    var index_queue_entries=0L
    for ( (_, count) <- logRefs ) {
      index_queue_entries += count.get()
    }

    // Don't force compactions until level 0 is full.
    val SSL_FILE_SIZE = 1024*1024*4L
    if( index_usage > SSL_FILE_SIZE*10 ) {
      if ( index_queue_entries > 0 ) {
        val ratio = (index_usage*1.0f/index_queue_entries)
        // println("usage: index_usage:%d, index_queue_entries:%d, ratio: %f".format(index_usage, index_queue_entries, ratio))

        // lets compact if we go way over the healthy ratio.
        if( ratio > store.autoCompactionRatio ) {
          index.compact_needed = true
        }
      } else {
        // at most the index should have 1 full level file.
        index.compact_needed = true
      }
    }

  }

  def gc(topicPositions:Seq[(Long, Long)]):Unit = {

    // Delete message refs for topics who's consumers have advanced..
    if( !topicPositions.isEmpty ) {
      might_fail_using_index {
        index.write(new WriteOptions, max_index_write_latency) { batch =>
          for( (topic, first) <- topicPositions ) {
            val ro = new ReadOptions
            ro.fillCache(true)
            ro.verifyChecksums(verifyChecksums)
            val start = encodeEntryKey(ENTRY_PREFIX, topic, 0)
            val end =  encodeEntryKey(ENTRY_PREFIX, topic, first)
            debug("Topic: %d GC to seq: %d", topic, first)
            index.cursorRange(start, end, ro) { case (key, value) =>
              val entry = EntryRecord.FACTORY.parseUnframed(value)
              batch.delete(key)
              logRefDecrement(entry.getValueLocation)
              true
            }
          }
        }
      }
    }

    detect_if_compact_needed

    // Lets compact the leveldb index if it looks like we need to.
    if( index.compact_needed ) {
      val start = System.nanoTime()
      index.compact
      val duration = System.nanoTime() - start;
      info("Compacted the leveldb index at: %s in %.2f ms", dirtyIndexFile, (duration / 1000000.0))
    }

    import collection.JavaConversions._

    // drop the logs that are no longer referenced.
    for( (x,y) <- logRefs.toSeq ) {
      if( y.get() <= 0 ) {
        if( y.get() < 0 ) {
          warn("Found a negative log reference for log: "+x)
        }
        debug("Log no longer referenced: %x", x)
        logRefs.remove(x)
      }
    }

    val emptyJournals = log.log_infos.keySet.toSet -- logRefs.keySet

    // We don't want to delete any journals that the index has not snapshot'ed or
    // the the

    var limit = oldest_retained_snapshot
    val deleteLimit = logRefKey(limit).getOrElse(limit).min(log.appender_start)

    emptyJournals.foreach { id =>
      if ( id < deleteLimit ) {
        debug("Deleting log at %x", id)
        log.delete(id)
      }
    }
  }

  def oldest_retained_snapshot = lastIndexSnapshotPos

  def removePlist(collectionKey: Long) = {
    val entryKeyPrefix = encodeLong(collectionKey)
    collectionMeta.remove(collectionKey)
    might_fail {
      val ro = new ReadOptions
      ro.fillCache(false)
      ro.verifyChecksums(false)
      plist.cursorPrefixed(entryKeyPrefix, ro) { (key, value)=>
        plist.delete(key)
        true
      }
    }
  }

  def plistPut(key:Array[Byte], value:Array[Byte]) = plist.put(key, value, PLIST_WRITE_OPTIONS)
  def plistDelete(key:Array[Byte]) = plist.delete(key, PLIST_WRITE_OPTIONS)
  def plistGet(key:Array[Byte]) = plist.get(key)
  def plistIterator = plist.db.iterator()

}
