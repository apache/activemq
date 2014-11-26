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

import org.apache.activemq.leveldb.{LevelDBStoreTest, LevelDBClient, LevelDBStore}
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
import scala.beans.BeanProperty
import java.util.concurrent.{CountDownLatch, TimeUnit}
import javax.management.ObjectName
import org.apache.activemq.broker.jmx.AnnotatedMBean

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

  var status = "initialized"

  override def createClient = new LevelDBClient(this) {
    // We don't want to start doing index snapshots until
    // he slave is caught up.
    override def post_log_rotate: Unit = {
      if( caughtUp ) {
        writeExecutor {
          snapshotIndex(false)
        }
      }
    }

    // The snapshots we create are based on what has been replayed.
    override def nextIndexSnapshotPos:Long = indexRecoveryPosition
  }

  override def doStart() = {
    queue.setLabel("slave: "+node_id)
    client.init()
    if (purgeOnStatup) {
      purgeOnStatup = false
      db.client.locked_purge
      info("Purged: "+this)
    }
    db.client.dirtyIndexFile.recursiveDelete
    db.client.plistIndexFile.recursiveDelete
    start_slave_connections

    if( java.lang.Boolean.getBoolean("org.apache.activemq.leveldb.test") ) {
      val name = new ObjectName(objectName.toString + ",view=Test")
      AnnotatedMBean.registerMBean(brokerService.getManagementContext, new LevelDBStoreTest(this), name)
    }
  }

  var stopped = false
  override def doStop(stopper: ServiceStopper) = {
    if( java.lang.Boolean.getBoolean("org.apache.activemq.leveldb.test") )
      brokerService.getManagementContext().unregisterMBean(new ObjectName(objectName.toString+",view=Test"));

    val latch = new CountDownLatch(1)
    stop_connections(^{
      latch.countDown
    })
    // Make sure the sessions are stopped before we close the client.
    latch.await()
    client.stop()
  }


  def restart_slave_connections = {
    stop_connections(^{
      client.stop()
      client = createClient
      client.init()
      start_slave_connections
    })
  }

  def start_slave_connections = {
    val transport: TcpTransport = create_transport

    status = "Attaching to master: "+connect
    info(status)
    wal_session = new Session(transport, (session)=>{
      // lets stash away our current state so that we can unstash it
      // in case we don't get caught up..  If the master dies,
      // the stashed data might be the best option to become the master.
      stash(directory)
      delete_store(directory)
      debug("Log replication session connected")
      session.request_then(SYNC_ACTION, null) { body =>
        val response = JsonCodec.decode(body, classOf[SyncResponse])
        transfer_missing(response)
        session.handler = wal_handler(session)
      }
    })
    wal_session.start
  }

  def create_transport: TcpTransport = {
    val transport = new TcpTransport()
    transport.setBlockingExecutor(blocking_executor)
    transport.setDispatchQueue(queue)
    transport.connecting(new URI(connect), null)
    transport
  }

  def stop_connections(cb:Task) = {
    var task = ^{
      unstash(directory)
      cb.run()
    }
    val wal_session_copy = wal_session
    if( wal_session_copy !=null ) {
      wal_session = null
      val next = task
      task = ^{
        wal_session_copy.transport.stop(next)
      }
    }
    val transfer_session_copy = transfer_session
    if( transfer_session_copy !=null ) {
      transfer_session = null
      val next = task
      task = ^{
        transfer_session_copy.transport.stop(next)
      }
    }
    task.run();
  }


  var wal_append_position = 0L
  var wal_append_offset = 0L
  @volatile
  var wal_date = 0L

  def send_wal_ack = {
    queue.assertExecuting()
    if( caughtUp && !stopped && wal_session!=null) {
      val ack = new WalAck()
      ack.position = wal_append_position
//      info("Sending ack: "+wal_append_position)
      wal_session.send_replication_frame(ACK_ACTION, ack)
      if( replay_from != ack.position ) {
        val old_replay_from = replay_from
        replay_from = ack.position
        client.writeExecutor {
          client.replay_from(old_replay_from, ack.position, false)
        }
      }
    }
  }

  val pending_log_removes = new util.ArrayList[Long]()

  def wal_handler(session:Session): (AnyRef)=>Unit = (command)=>{
    command match {
      case command:ReplicationFrame =>
        command.action match {
          case WAL_ACTION =>
            val value = JsonCodec.decode(command.body, classOf[LogWrite])
            if( caughtUp && value.offset ==0 && value.file!=0 ) {
              client.log.rotate
            }
            trace("%s, Slave WAL update: (file:%s, offset: %d, length: %d)".format(directory, value.file.toHexString, value.offset, value.length))
            val file = client.log.next_log(value.file)
            val buffer = map(file, value.offset, value.length, false)

            def readData = session.codec.readData(buffer, ^{
              if( value.sync ) {
                buffer.force()
              }

              unmap(buffer)
              wal_append_offset = value.offset+value.length
              wal_append_position = value.file + wal_append_offset
              wal_date = value.date
              if( !stopped ) {
                if( caughtUp ) {
                  client.log.current_appender.skip(value.length)
                }
                send_wal_ack
              }
            })

            if( client.log.recordLogTestSupport!=null ) {
              client.log.recordLogTestSupport.writeCall.call {
                readData
              }
            } else {
              readData
            }

          case LOG_DELETE_ACTION =>

            val value = JsonCodec.decode(command.body, classOf[LogDelete])
            if( !caughtUp ) {
              pending_log_removes.add(value.log)
            } else {
              client.log.delete(value.log)
            }

          case OK_ACTION =>
            // This comes in as response to a disconnect we send.
          case _ => session.fail("Unexpected command action: "+command.action)
        }
    }
  }

  class Session(transport:Transport, on_login: (Session)=>Unit) extends TransportHandler(transport) {

    val response_callbacks = new util.LinkedList[(ReplicationFrame)=>Unit]()

    override def onTransportFailure(error: IOException) {
      if( isStarted ) {
        warn("Unexpected session error: "+error)
        queue.after(1, TimeUnit.SECONDS) {
          if( isStarted ) {
            restart_slave_connections
          }
        }
      }
      super.onTransportFailure(error)
    }

    override def onTransportConnected {
      super.onTransportConnected
      val login = new Login
      login.security_token = securityToken
      login.node_id = node_id
      request_then(LOGIN_ACTION, login) { body =>
        on_login(Session.this)
      }
    }

    def disconnect(cb:Task) = queue {
      send_replication_frame(DISCONNECT_ACTION, null)
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
      send_replication_frame(action, body)
    }

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

    val dirty_index = client.dirtyIndexFile
    dirty_index.recursiveDelete

    val snapshot_index = client.snapshotIndexFile(state.snapshot_position)

    val transport = new TcpTransport()
    transport.setBlockingExecutor(blocking_executor)
    transport.setDispatchQueue(queue)
    transport.connecting(new URI(connect), null)

    debug("%s: Connecting download session. Snapshot index at: %s".format(directory, state.snapshot_position.toHexString))
    transfer_session = new Session(transport, (session)=> {

      var total_files = 0
      var total_size = 0L
      var downloaded_size = 0L
      var downloaded_files = 0

      def update_download_status = {
        status = "Attaching... Downloaded %.2f/%.2f kb and %d/%d files".format(downloaded_size/1024f, total_size/1024f, downloaded_files, total_files)
        info(status)
      }

      debug("Download session connected...")

      // Transfer the log files..
      var append_offset = 0L
      for( x <- state.log_files ) {

        if( x.file == state.append_log ) {
          append_offset = x.length
        }

        val stashed_file: File = directory / "stash" / x.file
        val target_file: File = directory / x.file

        def previously_downloaded:Boolean = {
          if( !stashed_file.exists() )
            return false

          if (stashed_file.length() < x.length )
            return false

          if (stashed_file.length() == x.length )
            return stashed_file.cached_crc32 == x.crc32

          if( x.file == state.append_log ) {
            return false;
          }

          return stashed_file.cached_crc32 == x.crc32
        }

        // We don't have to transfer log files that have been previously transferred.
        if( previously_downloaded ) {
          // lets link it from the stash directory..
          info("Slave skipping download of: log/"+x.file)
          if( x.file == state.append_log ) {
            stashed_file.copyTo(target_file) // let not link a file that's going to be modified..
          } else {
            stashed_file.linkTo(target_file)
          }
        } else {
          val transfer = new Transfer()
          transfer.file = "log/"+x.file
          transfer.offset = 0
          transfer.length = x.length
          debug("Slave requested: "+transfer.file)
          total_size += x.length
          total_files += 1
          session.request_then(GET_ACTION, transfer) { body =>
            val buffer = map(target_file, 0, x.length, false)
            session.codec.readData(buffer, ^{
              unmap(buffer)
              trace("%s, Downloaded %s, offset:%d, length:%d", directory, transfer.file, transfer.offset, transfer.length)
              downloaded_size += x.length
              downloaded_files += 1
              update_download_status
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
        total_size += x.length
        total_files += 1
        session.request_then(GET_ACTION, transfer) { body =>
          val buffer = map(dirty_index / x.file, 0, x.length, false)
          session.codec.readData(buffer, ^{
            unmap(buffer)
            trace("%s, Downloaded %s, offset:%d, length:%d", directory, transfer.file, transfer.offset, transfer.length)
            downloaded_size += x.length
            downloaded_files += 1
            update_download_status
          })
        }
      }

      session.request_then(DISCONNECT_ACTION, null) { body =>
        // Ok we are now caught up.
        status = "Attached"
        info(status)
        stash_clear(directory) // we don't need the stash anymore.
        transport.stop(NOOP)
        transfer_session = null
        replay_from = state.snapshot_position
        if( wal_append_position < state.wal_append_position ) {
          wal_append_position = state.wal_append_position
          wal_append_offset = append_offset
        }
        client.writeExecutor {
          if( !state.index_files.isEmpty ) {
            trace("%s: Index sync complete, copying to snapshot.", directory)
            client.copyDirtyIndexToSnapshot(state.wal_append_position)
          }
          client.replay_init()
        }
        caughtUp = true
        client.log.open(wal_append_offset)
        send_wal_ack
        for( i <- pending_log_removes ) {
          client.log.delete(i);
        }
        pending_log_removes.clear()
      }
    })
    transfer_session.start
    state.snapshot_position
  }


}
