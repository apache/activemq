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
import java.util
import org.fusesource.hawtdispatch._
import org.apache.activemq.leveldb.replicated.dto._
import org.fusesource.hawtdispatch.transport._
import java.util.concurrent._
import java.io.{IOException, File}
import java.net.{InetSocketAddress, URI}
import java.util.concurrent.atomic.AtomicLong
import scala.reflect.BeanProperty
import java.util.UUID

class PositionSync(val position:Long, count:Int) extends CountDownLatch(count)

object MasterLevelDBStore extends Log

/**
 */
class MasterLevelDBStore extends LevelDBStore {

  import MasterLevelDBStore._
  import collection.JavaConversions._
  import ReplicationSupport._

  @BeanProperty
  var bind = "tcp://0.0.0.0:61619"
  @BeanProperty
  var securityToken = ""
  @BeanProperty
  var minReplica = 1

  val slaves = new ConcurrentHashMap[String,SlaveState]()

  def replicaId:String = {
    val replicaid_file = directory / "replicaid.txt"
    if( replicaid_file.exists() ) {
      replicaid_file.readText()
    } else {
      val rc = UUID.randomUUID().toString
      replicaid_file.getParentFile.mkdirs()
      replicaid_file.writeText(rc)
      rc
    }
  }

  override def doStart = {
    super.doStart
    start_protocol_server
  }

  override def doStop(stopper: ServiceStopper): Unit = {
    if( transport_server!=null ) {
      transport_server.start(NOOP)
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

  def start_protocol_server = {
    transport_server = new TcpTransportServer(new URI(bind))
    transport_server.setBlockingExecutor(blocking_executor)
    transport_server.setDispatchQueue(createQueue("replication server"))
    transport_server.setTransportServerListener(new TransportServerListener(){
      def onAccept(transport: Transport) {
        transport.setDispatchQueue(createQueue("connection from "+transport.getRemoteAddress))
        transport.setBlockingExecutor(blocking_executor)
        new Session(transport)
      }
      def onAcceptError(error: Exception) {
        warn(error)
      }
    })
    val start_latch = new CountDownLatch(1)
    transport_server.start(^{
      start_latch.countDown()
    })
    start_latch.await()
  }

  def getPort = transport_server.getSocketAddress.asInstanceOf[InetSocketAddress].getPort

  def stop_protocol_server = {
    transport_server.stop(NOOP)
  }


  case class HawtCallback[T](cb:(T)=>Unit) extends Function1[T, Unit] {
    val queue = getCurrentQueue
    def apply(value:T) = {
      if( queue==null || queue.isExecuting ) {
        cb(value)
      } else {
        queue {
          cb(value)
        }
      }
    }
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
      info("handle_sync")
      slave_state = slaves.get(login.slave_id)
      if ( slave_state == null ) {
        slave_state = new SlaveState(login.slave_id)
        slaves.put(login.slave_id, slave_state)
      }
      slave_state.start(Session.this)
    }

    def handle_ack(req:WalAck):Unit = {
      if( login == null || slave_state == null) {
        return;
      }
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
      send(FileTransferFrame(file, req.offset, req.length))
    }

  }

  class SlaveState(val slave_id:String) {

    var held_snapshot:Option[Long] = None
    var session:Session = _
    var position = new AtomicLong(0)

    def start(session:Session) = {
      info("SlaveState:start")

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

    def replicate_wal(frame1:ReplicationFrame, frame2:FileTransferFrame ) = {
      val h = this.synchronized {
        session
      }
      if( h !=null ) {
        h.queue {
          h.send(frame1)
          h.send(frame2)
        }
      }
    }

    def position_update(position:Long) = {
      val was = this.position.getAndSet(position)
      if( was == 0 ) {
        info("Slave has finished synchronizing: "+slave_id)
        this.synchronized {
          this.held_snapshot = None
        }
      }
      check_position_sync
    }

    @volatile
    var last_position_sync:PositionSync = null
    def check_position_sync = {
      val p = position_sync
      if( last_position_sync!=p ) {
        if( position.get >= p.position ) {
          p.countDown
          last_position_sync = p
        }
      }
    }
  }

  @volatile
  var position_sync = new PositionSync(0L, 0)

  def wal_sync_to(position:Long):Unit = {
    if( minReplica<1 ) {
      return
    }
    val position_sync = new PositionSync(position, minReplica)
    this.position_sync = position_sync
    for( slave <- slaves.values() ) {
      slave.check_position_sync
    }
    while( !position_sync.await(1, TimeUnit.SECONDS) ) {
      println("Waiting for slaves to ack log position: "+position_sync.position)
      for( slave <- slaves.values() ) {
        slave.check_position_sync
      }
    }
  }

  def replicate_wal(file:File, position:Long, offset:Long, length:Long):Unit = {
    if( length > 0 ) {
      val value = new LogWrite
      value.file = position;
      value.offset = offset;
      value.length = length
      val frame1 = ReplicationFrame(WAL_ACTION, JsonCodec.encode(value))
      val frame2 = FileTransferFrame(file, offset, length)
      for( slave <- slaves.values() ) {
        slave.replicate_wal(frame1, frame2)
      }
    }
  }

}
