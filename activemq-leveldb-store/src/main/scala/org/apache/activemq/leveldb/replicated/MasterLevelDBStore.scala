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
import org.apache.activemq.leveldb.util.FileSupport._
import org.apache.activemq.leveldb.util.{JsonCodec, Log}
import org.fusesource.hawtdispatch._
import org.apache.activemq.leveldb.replicated.dto._
import org.fusesource.hawtdispatch.transport._
import java.util.concurrent._
import java.io.{IOException, File}
import java.net.{SocketAddress, InetSocketAddress, URI}
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import scala.beans.BeanProperty
import org.fusesource.hawtbuf.{Buffer, AsciiBuffer}

class PositionSync(val position:Long, count:Int) extends CountDownLatch(count)

object MasterLevelDBStore extends Log {

  val SYNC_TO_DISK = 0x01
  val SYNC_TO_REMOTE = 0x02
  val SYNC_TO_REMOTE_MEMORY = 0x04 | SYNC_TO_REMOTE
  val SYNC_TO_REMOTE_DISK = 0x08 | SYNC_TO_REMOTE

}

case class SlaveStatus(nodeId:String, remoteAddress:String, attached:Boolean, position:Long)

/**
 */
class MasterLevelDBStore extends LevelDBStore with ReplicatedLevelDBStoreTrait {

  import MasterLevelDBStore._
  import collection.JavaConversions._
  import ReplicationSupport._

  @BeanProperty
  var bind = "tcp://0.0.0.0:61619"

  @BeanProperty
  var replicas = 3
  def minSlaveAcks = replicas/2

  var _syncTo="quorum_mem"
  var syncToMask=SYNC_TO_REMOTE_MEMORY

  @BeanProperty
  def syncTo = _syncTo
  @BeanProperty
  def syncTo_=(value:String) {
    _syncTo = value
    syncToMask = 0
    for( v <- value.split(",").map(_.trim.toLowerCase) ) {
      v match {
        case "" =>
        case "local_mem" =>
        case "local_disk" => syncToMask |= SYNC_TO_DISK
        case "remote_mem" => syncToMask |= SYNC_TO_REMOTE_MEMORY
        case "remote_disk" => syncToMask |= SYNC_TO_REMOTE_DISK
        case "quorum_mem" => syncToMask |= SYNC_TO_REMOTE_MEMORY
        case "quorum_disk" => syncToMask |= SYNC_TO_REMOTE_DISK | SYNC_TO_DISK
        case x => warn("Unknown syncTo value: [%s]", x)
      }
    }
  }

  val slaves = new ConcurrentHashMap[String,SlaveState]()

  def slaves_status = slaves.values().map(_.status)

  def status = {
    var caughtUpCounter = 0
    var notCaughtUpCounter = 0
    for( slave <- slaves.values() ) {
      if( slave.isCaughtUp ) {
        caughtUpCounter += 1
      } else {
        notCaughtUpCounter += 1
      }
    }
    var rc = ""
    if( notCaughtUpCounter > 0 ) {
      rc += "%d slave nodes attaching. ".format(notCaughtUpCounter)
    }
    if( caughtUpCounter > 0 ) {
      rc += "%d slave nodes attached. ".format(caughtUpCounter)
    }
    rc
  }

  override def doStart = {
    unstash(directory)
    super.doStart
    start_protocol_server
    // Lets not complete the startup until at least one slave is synced up.
    wal_sync_to(wal_append_position)
  }

  override def doStop(stopper: ServiceStopper): Unit = {
    if( transport_server!=null ) {
      stop_protocol_server
      transport_server = null
    }
    super.doStop(stopper)
  }

  override def createClient = new MasterLevelDBClient(this)
  def master_client = client.asInstanceOf[MasterLevelDBClient]

  //////////////////////////////////////
  // Replication Protocol Stuff
  //////////////////////////////////////
  var transport_server:TransportServer = _
  val start_latch = new CountDownLatch(1)

  def start_protocol_server = {
    transport_server = new TcpTransportServer(new URI(bind))
    transport_server.setBlockingExecutor(blocking_executor)
    transport_server.setDispatchQueue(createQueue("master: "+node_id))
    transport_server.setTransportServerListener(new TransportServerListener(){
      def onAccept(transport: Transport) {
        transport.setDispatchQueue(createQueue("connection from "+transport.getRemoteAddress))
        transport.setBlockingExecutor(blocking_executor)
        new Session(transport).start

      }
      def onAcceptError(error: Exception) {
        warn(error)
      }
    })
    transport_server.start(^{
      start_latch.countDown()
    })
    start_latch.await()
  }

  def getPort = {
    start_latch.await()
    transport_server.getSocketAddress.asInstanceOf[InetSocketAddress].getPort
  }

  def stop_protocol_server = {
    transport_server.stop(NOOP)
  }

  class Session(transport: Transport) extends TransportHandler(transport) {

    var login:Login = _
    var slave_state:SlaveState = _
    var disconnected = false

    def queue = transport.getDispatchQueue

    override def onTransportFailure(error: IOException) {
      if( !disconnected ) {
        warn("Unexpected session error: "+error)
      }
      super.onTransportFailure(error)
    }

    def onTransportCommand(command: Any) = {
      command match {
        case command:ReplicationFrame =>
          command.action match {
            case LOGIN_ACTION =>
              handle_login(JsonCodec.decode(command.body, classOf[Login]))
            case SYNC_ACTION =>
              handle_sync()
            case GET_ACTION =>
              handle_get(JsonCodec.decode(command.body, classOf[Transfer]))
            case ACK_ACTION =>
              handle_ack(JsonCodec.decode(command.body, classOf[WalAck]))
            case DISCONNECT_ACTION =>
              handle_disconnect()
            case _ =>
              sendError("Unknown frame action: "+command.action)
          }
      }
    }

    def handle_login(request:Login):Unit = {
      if( request.security_token != securityToken ) {
        sendError("Invalid security_token");
      } else {
        login = request;
        sendOk(null)
      }
    }

    override def onTransportDisconnected() {
      val slave_state = this.slave_state;
      if( slave_state !=null ) {
        this.slave_state=null
        if( slave_state.stop(this) && isStarted ) {
          slaves.remove(slave_state.slave_id, slave_state)
        }
      }
    }

    def handle_disconnect():Unit = {
      disconnected = true;
      sendOk(null)
    }

    def handle_sync():Unit = {
      if( login == null ) {
        sendError("Not logged in")
        return;
      }
      debug("handle_sync")
      slave_state = slaves.get(login.node_id)
      if ( slave_state == null ) {
        slave_state = new SlaveState(login.node_id)
        slaves.put(login.node_id, slave_state)
      }
      slave_state.start(Session.this)
    }

    def handle_ack(req:WalAck):Unit = {
      if( login == null || slave_state == null) {
        return;
      }
      trace("%s: Got WAL ack, position: %d, from: %s", directory, req.position, slave_state.slave_id)
      slave_state.position_update(req.position)
    }

    def handle_get(req:Transfer):Unit = {
      if( login == null ) {
        sendError("Not logged in")
        return;
      }

      val file = if( req.file.startsWith("log/" ) ) {
        client.logDirectory / req.file.stripPrefix("log/")
      } else {
        client.directory / req.file
      }

      if( !file.exists() ) {
        sendError("file does not exist")
        return
      }
      val length = file.length()

      if( req.offset > length ) {
        sendError("Invalid offset")
        return
      }
      if( req.offset+req.length > length ) {
        sendError("Invalid length")
      }
      sendOk(null)
      send(new FileTransferFrame(file, req.offset, req.length))
    }

  }

  class SlaveState(val slave_id:String) {

    var held_snapshot:Option[Long] = None
    var session:Session = _
    var position = new AtomicLong(0)
    var caughtUp = new AtomicBoolean(false)
    var socketAddress:SocketAddress = _

    def start(session:Session) = {
      debug("SlaveState:start")
      socketAddress = session.transport.getRemoteAddress
      session.queue.setLabel(transport_server.getDispatchQueue.getLabel+" -> "+slave_id)

      val resp = this.synchronized {
        if( this.session!=null ) {
          this.session.transport.stop(NOOP)
        }

        this.session = session
        val snapshot_id = client.lastIndexSnapshotPos
        held_snapshot = Option(snapshot_id)
        position.set(0)
        master_client.snapshot_state(snapshot_id)
      }
      info("Slave has connected: "+slave_id)
      session.queue {
        session.sendOk(resp)
      }
    }

    def stop(session:Session) = {
      this.synchronized {
        if( this.session == session ) {
          info("Slave has disconnected: "+slave_id)
          true
        } else {
          false
        }
      }
    }

    def queue(func: (Session)=>Unit) = {
      val h = this.synchronized {
        session
      }
      if( h !=null ) {
        h.queue {
          func(session)
        }
      }
    }

    def replicate(value:LogDelete):Unit = {
      val frame = new ReplicationFrame(LOG_DELETE_ACTION, JsonCodec.encode(value))
      queue { session =>
        session.send(frame)
      }
    }

    var unflushed_replication_frame:DeferredReplicationFrame = null

    class DeferredReplicationFrame(file:File, val position:Long, _offset:Long, initialLength:Long) extends ReplicationFrame(WAL_ACTION, null) {
      val fileTransferFrame = new FileTransferFrame(file, _offset, initialLength)
      var encoded:Buffer = null

      def offset = fileTransferFrame.offset
      def length = fileTransferFrame.length

      override def body: Buffer = {
        if( encoded==null ) {
          val value = new LogWrite
          value.file = position;
          value.offset = offset;
          value.sync = (syncToMask & SYNC_TO_REMOTE_DISK)!=0
          value.length = fileTransferFrame.length
          value.date = date
          encoded = JsonCodec.encode(value)
        }
        encoded
      }
    }

    def replicate(file:File, position:Long, offset:Long, length:Long):Unit = {
      queue { session =>

        // Check to see if we can merge the replication event /w the previous event..
        if( unflushed_replication_frame == null ||
                unflushed_replication_frame.position!=position ||
                (unflushed_replication_frame.offset+unflushed_replication_frame.length)!=offset ) {

          // We could not merge the replication event /w the previous event..
          val frame = new DeferredReplicationFrame(file, position, offset, length)
          unflushed_replication_frame = frame
          session.send(frame, ()=>{
            trace("%s: Sent WAL update: (file:%s, offset: %d, length: %d) to %s", directory, file, frame.offset, frame.length, slave_id)
            if( unflushed_replication_frame eq frame ) {
              unflushed_replication_frame = null
            }
          })
          session.send(frame.fileTransferFrame)

        } else {
          // We were able to merge.. yay!
          assert(unflushed_replication_frame.encoded == null)
          unflushed_replication_frame.fileTransferFrame.length += length
        }
      }
    }

    def position_update(position:Long) = {
      this.position.getAndSet(position)
      check_position_sync
    }

    @volatile
    var last_position_sync:PositionSync = null
    def check_position_sync = {
      val p = position_sync
      if( last_position_sync!=p ) {
        if( position.get >= p.position ) {
          if( caughtUp.compareAndSet(false, true) ) {
            info("Slave has now caught up: "+slave_id)
            this.synchronized {
              this.held_snapshot = None
            }
          }
          p.countDown
          last_position_sync = p
        }
      }
    }

    def isCaughtUp = caughtUp.get()

    def status = SlaveStatus(slave_id, socketAddress.toString, isCaughtUp, position.get())
  }

  @volatile
  var position_sync = new PositionSync(0L, 0)

  def wal_sync_to(position:Long):Unit = {
    if( minSlaveAcks<1 || (syncToMask & SYNC_TO_REMOTE)==0) {
      return
    }

    if( isStoppedOrStopping ) {
      throw new IllegalStateException("Store replication stopped")
    }

    val position_sync = new PositionSync(position, minSlaveAcks)
    this.position_sync = position_sync
    for( slave <- slaves.values() ) {
      slave.check_position_sync
    }

    while( !position_sync.await(1, TimeUnit.SECONDS) ) {
      if( isStoppedOrStopping ) {
        throw new IllegalStateException("Store replication stopped")
      }
      warn("Store update waiting on %d replica(s) to catch up to log position %d. %s", minSlaveAcks, position, status)
    }
  }


  def isStoppedOrStopping: Boolean = {
    if( isStopped || isStopping )
      return true
    if( broker_service!=null && broker_service.isStopping )
      return true
    false
  }

  def date = System.currentTimeMillis()

  def replicate_wal(file:File, position:Long, offset:Long, length:Long):Unit = {
    if( length > 0 ) {
      for( slave <- slaves.values() ) {
        slave.replicate(file, position, offset, length)
      }
    }
  }

  def replicate_log_delete(log:Long):Unit = {
    val value = new LogDelete
    value.log = log
    for( slave <- slaves.values() ) {
      slave.replicate(value)
    }
  }

  def wal_append_position = client.wal_append_position
  @volatile
  var wal_date = 0L
}
