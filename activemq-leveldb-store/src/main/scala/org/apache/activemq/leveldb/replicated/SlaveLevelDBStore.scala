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

import org.apache.activemq.leveldb.LevelDBStore
import org.apache.activemq.util.ServiceStopper
import java.util
import org.fusesource.hawtdispatch._
import org.apache.activemq.leveldb.replicated.dto._
import org.fusesource.hawtdispatch.transport._
import java.net.URI
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}
import org.apache.activemq.leveldb.util._

import FileSupport._
import java.io.{IOException, RandomAccessFile, File}
import scala.reflect.BeanProperty

object SlaveLevelDBStore extends Log

/**
 */
class SlaveLevelDBStore extends LevelDBStore with ReplicatedLevelDBStoreTrait {

  import SlaveLevelDBStore._
  import ReplicationSupport._
  import collection.JavaConversions._

  @BeanProperty
  var connect = "tcp://0.0.0.0:61619"

  val queue = createQueue("leveldb replication slave")
  var replay_from = 0L
  var caughtUp = false

  var wal_session:Session = _
  var transfer_session:Session = _

  override def doStart() = {
    client.init()

    if (purgeOnStatup) {
      purgeOnStatup = false
      db.client.locked_purge
      info("Purged: "+this)
    }

    val transport = new TcpTransport()
    transport.setBlockingExecutor(blocking_executor)
    transport.setDispatchQueue(queue)
    transport.connecting(new URI(connect), null)

    info("Connecting to master...")
    wal_session = new Session(transport, (session)=>{
      debug("Connected to master.  Syncing")
      session.request_then(SYNC_ACTION, null) { body =>
        val response = JsonCodec.decode(body, classOf[SyncResponse])
        transfer_missing(response)
        session.handler = wal_handler(session)
      }
    })
  }
  var stopped = false
  override def doStop(stopper: ServiceStopper) = {
    val latch = RetainedLatch()
    if( wal_session !=null ) {
      wal_session.disconnect(latch.retain)
      wal_session = null
    }
    if( transfer_session !=null ) {
      transfer_session.disconnect(latch.retain)
      transfer_session = null
    }
    queue {
      stopped = true
      latch.release
    }
    // Make sure the sessions are stopped before we close the client.
    latch.await()
    db.client.stop()
  }

  var wal_append_position = 0L
  var wal_append_offset = 0L

  def send_wal_ack = {
    queue.assertExecuting()
    if( caughtUp && !stopped && wal_session!=null) {
      val ack = new WalAck()
      ack.position = wal_append_position
//      info("Sending ack: "+wal_append_position)
      wal_session.send(ACK_ACTION, ack)
      if( replay_from != ack.position ) {
        val old_replay_from = replay_from
        replay_from = ack.position
        client.writeExecutor {
          client.replay_from(old_replay_from, ack.position)
        }
      }
    }
  }

  def wal_handler(session:Session): (AnyRef)=>Unit = (command)=>{
    command match {
      case command:ReplicationFrame =>
        command.action match {
          case WAL_ACTION =>
            val value = JsonCodec.decode(command.body, classOf[LogWrite])
            if( caughtUp && value.offset ==0 ) {
              client.log.rotate
            }
            val file = client.log.next_log(value.file)
            val buffer = map(file, value.offset, value.length, false)
            session.codec.readData(buffer, ^{
              unmap(buffer)
//              info("Slave WAL update: %s, (offset: %d, length: %d), sending ack:%s", file, value.offset, value.length, caughtUp)
              wal_append_offset = value.offset+value.length
              wal_append_position = value.file + wal_append_offset
              if( !stopped ) {
                if( caughtUp ) {
                  client.log.current_appender.skip(value.length)
                }
                send_wal_ack
              }
            })
          case OK_ACTION =>
            // This comes in as response to a disconnect we send.
          case _ => session.fail("Unexpected command action: "+command.action)
        }
    }
  }

  class Session(transport:Transport, on_login: (Session)=>Unit) extends TransportHandler(transport) {

    override def onTransportFailure(error: IOException) {
      if( isStarted ) {
        warn("Unexpected session error: "+error)
      }
      super.onTransportFailure(error)
    }

    override def onTransportConnected {
      super.onTransportConnected
      val login = new Login
      login.security_token = securityToken
      login.slave_id = replicaId
      request_then(LOGIN_ACTION, login) { body =>
        on_login(Session.this)
      }
    }

    def disconnect(cb:Task) = queue {
      send(DISCONNECT_ACTION, null)
      transport.flush()
      transport.stop(cb)
    }

    def fail(msg:String) = {
      error(msg)
      transport.stop(NOOP)
    }

    var handler: (AnyRef)=>Unit = response_handler
    def onTransportCommand(command: AnyRef) = handler(command)

    def request_then(action:AsciiBuffer, body:AnyRef)(cb:(Buffer)=>Unit) = {
      request(action, body){ response =>
        response.action match {
          case OK_ACTION =>
            cb(response.body)
          case ERROR_ACTION =>
            fail(action+" failed: "+response.body.ascii().toString)
          case _ =>
            fail("Unexpected response action: "+response.action)
        }
      }
    }

    def request(action:AsciiBuffer, body:AnyRef)(cb:(ReplicationFrame)=>Unit) = {
      response_callbacks.addLast(cb)
      send(action, body)
    }
    val response_callbacks = new util.LinkedList[(ReplicationFrame)=>Unit]()
    def response_handler: (AnyRef)=>Unit = (command)=> {
      command match {
        case command:ReplicationFrame =>
          if( response_callbacks.isEmpty ) {
            error("No response callback registered")
            transport.stop(NOOP)
          } else {
            val callback = response_callbacks.removeFirst()
            callback(command)
          }
      }
    }
  }

  def transfer_missing(state:SyncResponse) = {
    // Start up another connection to catch sync
    // up the missing data
    val log_dir = client.logDirectory
    val dirty_index = client.dirtyIndexFile
    dirty_index.recursiveDelete

    val snapshot_index = client.snapshotIndexFile(state.snapshot_position)

    val transport = new TcpTransport()
    transport.setBlockingExecutor(blocking_executor)
    transport.setDispatchQueue(queue)
    transport.connecting(new URI(connect), null)

    info("Connecting catchup session...")
    transfer_session = new Session(transport, (session)=> {
      info("Catchup session connected...")

      // Transfer the log files..
      var append_offset = 0L
      for( x <- state.log_files ) {
        if( x.file == state.append_log ) {
          append_offset = x.length
        }

        val target_file: File = log_dir / x.file

        def previously_downloaded:Boolean = {
          if( !target_file.exists() )
            return false

          if (target_file.length() < x.length )
            return false

          if (target_file.length() == x.length )
            return target_file.cached_crc32 == x.crc32

          if ( target_file.crc32(x.length) == x.crc32 ) {
            // we don't want to truncate the log file currently being appended to.
            if( x.file != state.append_log ) {
              // Our log file might be longer. lets truncate to match.
              val raf = new RandomAccessFile(target_file, "rw")
              try {
                raf.setLength(x.length)
              } finally {
                raf.close();
              }
            }
            return true;
          }
          return false
        }

        // We don't have to transfer log files that have been previously transferred.
        if( previously_downloaded ) {
          info("Slave skipping download of: log/"+x.file)
        } else {
          val transfer = new Transfer()
          transfer.file = "log/"+x.file
          transfer.offset = 0
          transfer.length = x.length
          debug("Slave requested: "+transfer.file)
          session.request_then(GET_ACTION, transfer) { body =>
            val buffer = map(target_file, 0, x.length, false)
            session.codec.readData(buffer, ^{
              unmap(buffer)
              info("Slave downloaded: "+transfer.file+" ("+x.length+" bytes)")
            })
          }
        }
      }

      // Transfer the index files..
      if( !state.index_files.isEmpty ) {
        dirty_index.mkdirs()
      }
      for( x <- state.index_files ) {
        val transfer = new Transfer()
        transfer.file = snapshot_index.getName+"/"+x.file
        transfer.offset = 0
        transfer.length = x.length
        info("Slave requested: "+transfer.file)
        session.request_then(GET_ACTION, transfer) { body =>
          val buffer = map(dirty_index / x.file, 0, x.length, false)
          session.codec.readData(buffer, ^{
            unmap(buffer)
            info("Slave downloaded: "+transfer.file+" ("+x.length+" bytes)")
          })
        }
      }

      session.request_then(DISCONNECT_ACTION, null) { body =>
        // Ok we are now caught up.
        info("Slave has now caught up")
        transport.stop(NOOP)
        transfer_session = null
        replay_from = state.snapshot_position
        if( wal_append_position < state.wal_append_position ) {
          wal_append_position = state.wal_append_position
          wal_append_offset = append_offset
        }
        client.writeExecutor {
          if( !state.index_files.isEmpty ) {
            client.copyDirtyIndexToSnapshot(state.wal_append_position)
          }
          client.replay_init()
        }
        caughtUp = true
        client.log.open(wal_append_offset)
        send_wal_ack
      }
    })
    state.snapshot_position
  }


}
