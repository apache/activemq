/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.leveldb.replicated

import org.linkedin.util.clock.Timespan
import scala.beans.BeanProperty
import org.apache.activemq.util.ServiceStopper
import org.apache.activemq.leveldb.{LevelDBClient, RecordLog, LevelDBStore}
import java.net.{NetworkInterface, InetAddress}
import org.fusesource.hawtdispatch._
import org.apache.activemq.broker.{LockableServiceSupport, Locker}
import org.apache.activemq.store.PersistenceAdapter
import java.util.concurrent.CountDownLatch
import java.util.concurrent.atomic.AtomicBoolean
import org.apache.activemq.leveldb.util.Log
import java.io.File
import org.apache.activemq.usage.SystemUsage
import org.apache.activemq.ActiveMQMessageAuditNoSync
import org.apache.activemq.broker.jmx.{OpenTypeSupport, BrokerMBeanSupport, AnnotatedMBean}
import javax.management.ObjectName
import javax.management.openmbean.{CompositeDataSupport, SimpleType, CompositeData}
import java.util
import org.apache.activemq.leveldb.replicated.groups._

object ElectingLevelDBStore extends Log {

  def machine_hostname: String = {
    import collection.JavaConversions._
    // Get the host name of the first non loop-back interface..
    for (interface <- NetworkInterface.getNetworkInterfaces; if (!interface.isLoopback); inet <- interface.getInetAddresses) {
      var address = inet.getHostAddress
      var name = inet.getCanonicalHostName
      if( address!= name ) {
        return name
      }
    }
    // Or else just go the simple route.
    return InetAddress.getLocalHost.getCanonicalHostName;
  }

}

/**
 *
 */
class ElectingLevelDBStore extends ProxyLevelDBStore {
  import ElectingLevelDBStore._

  def proxy_target = master

  @BeanProperty
  var zkAddress = "127.0.0.1:2181"
  @BeanProperty
  var zkPassword:String = _
  @BeanProperty
  var zkPath = "/default"
  @BeanProperty
  var zkSessionTimeout = "2s"

  var brokerName: String = _

  @BeanProperty
  var container: String = _

  @BeanProperty
  var hostname: String = _

  @BeanProperty
  var connectUrl: String = _

  @BeanProperty
  var bind = "tcp://0.0.0.0:61619"

  @BeanProperty
  var weight = 1
  @BeanProperty
  var replicas = 3
  @BeanProperty
  var sync="quorum_mem"

  def clusterSizeQuorum = (replicas/2) + 1

  @BeanProperty
  var securityToken = ""

  var directory = LevelDBStore.DEFAULT_DIRECTORY;
  override def setDirectory(dir: File) {
    directory = dir
  }
  override def getDirectory: File = {
    return directory
  }

  @BeanProperty
  var logSize: Long = 1024 * 1024 * 100
  @BeanProperty
  var indexFactory: String = "org.fusesource.leveldbjni.JniDBFactory, org.iq80.leveldb.impl.Iq80DBFactory"
  @BeanProperty
  var verifyChecksums: Boolean = false
  @BeanProperty
  var indexMaxOpenFiles: Int = 1000
  @BeanProperty
  var indexBlockRestartInterval: Int = 16
  @BeanProperty
  var paranoidChecks: Boolean = false
  @BeanProperty
  var indexWriteBufferSize: Int = 1024 * 1024 * 6
  @BeanProperty
  var indexBlockSize: Int = 4 * 1024
  @BeanProperty
  var indexCompression: String = "snappy"
  @BeanProperty
  var logCompression: String = "none"
  @BeanProperty
  var indexCacheSize: Long = 1024 * 1024 * 256L
  @BeanProperty
  var flushDelay = 0
  @BeanProperty
  var asyncBufferSize = 1024 * 1024 * 4
  @BeanProperty
  var monitorStats = false
  @BeanProperty
  var failoverProducersAuditDepth = ActiveMQMessageAuditNoSync.DEFAULT_WINDOW_SIZE;
  @BeanProperty
  var maxFailoverProducersToTrack = ActiveMQMessageAuditNoSync.MAXIMUM_PRODUCER_COUNT;

  var master: MasterLevelDBStore = _
  var slave: SlaveLevelDBStore = _

  var zk_client: ZKClient = _
  var zk_group: ZooKeeperGroup = _

  var position: Long = -1L


  override def toString: String = {
    return "Replicated LevelDB[%s, %s/%s]".format(directory.getAbsolutePath, zkAddress, zkPath)
  }

  var usageManager: SystemUsage = _
  override def setUsageManager(usageManager: SystemUsage) {
    this.usageManager = usageManager
  }

  def node_id = ReplicatedLevelDBStoreTrait.node_id(directory)

  def init() {

    if(brokerService!=null && brokerService.isUseJmx){
      try {
        AnnotatedMBean.registerMBean(brokerService.getManagementContext, new ReplicatedLevelDBStoreView(this), objectName)
      } catch {
        case e: Throwable => {
          warn(e, "PersistenceAdapterReplication could not be registered in JMX: " + e.getMessage)
        }
      }
    }

    // Figure out our position in the store.
    directory.mkdirs()
    val log = new RecordLog(directory, LevelDBClient.LOG_SUFFIX)
    log.logSize = logSize
    log.open()
    position = try {
      log.current_appender.append_position
    } finally {
      log.close
    }

    zk_client = new ZKClient(zkAddress, Timespan.parse(zkSessionTimeout), null)
    if( zkPassword!=null ) {
      zk_client.setPassword(zkPassword)
    }
    zk_client.start
    zk_client.waitForConnected(Timespan.parse("30s"))

    zk_group = ZooKeeperGroupFactory.create(zk_client, zkPath)
    val master_elector = new MasterElector(this)
    debug("Starting ZooKeeper group monitor")
    master_elector.start(zk_group)
    debug("Joining ZooKeeper group")
    master_elector.join

    this.setUseLock(true)
    this.setLocker(createDefaultLocker())
  }

  def createDefaultLocker(): Locker = new Locker {

    def setLockable(lockable: LockableServiceSupport) {}
    def configure(persistenceAdapter: PersistenceAdapter) {}
    def setFailIfLocked(failIfLocked: Boolean) {}
    def setLockAcquireSleepInterval(lockAcquireSleepInterval: Long) {}
    def setName(name: String) {}

    def start()  = {
      master_started_latch.await()
    }

    def keepAlive(): Boolean = {
      master_started.get()
    }

    def stop() {}
  }


  val master_started_latch = new CountDownLatch(1)
  val master_started = new AtomicBoolean(false)

  def start_master(func: (Int) => Unit) = {
    assert(master==null)
    master = create_master()
    master_started.set(true)
    master.blocking_executor.execute(^{
      master.start();
      master_stopped.set(false)
      master_started_latch.countDown()
    })
    master.blocking_executor.execute(^{
      func(master.getPort)
    })
  }

  def isMaster = master_started.get() && !master_stopped.get()

  val stopped_latch = new CountDownLatch(1)
  val master_stopped = new AtomicBoolean(false)

  def stop_master(func: => Unit) = {
    assert(master!=null)
    master.blocking_executor.execute(^{
      master.stop();
      master_stopped.set(true)
      position = master.wal_append_position
      stopped_latch.countDown()
      master = null
      func
    })
    master.blocking_executor.execute(^{
      val broker = brokerService
      if( broker!=null ) {
        try {
            broker.requestRestart();
            broker.stop();
        } catch {
          case e:Exception=> warn("Failure occurred while restarting the broker", e);
        }
      }
    })
  }

  def objectName = {
    var objectNameStr = BrokerMBeanSupport.createPersistenceAdapterName(brokerService.getBrokerObjectName.toString, "LevelDB[" + directory.getAbsolutePath + "]").toString
    objectNameStr += "," + "view=Replication";
    new ObjectName(objectNameStr);
  }

  protected def doStart() = {
    master_started_latch.await()
  }

  protected def doStop(stopper: ServiceStopper) {
    if(brokerService!=null && brokerService.isUseJmx){
      brokerService.getManagementContext().unregisterMBean(objectName);
    }
    if (zk_group != null) {
      zk_group.close
      zk_group = null
    }
    if (zk_client != null) {
      zk_client.close()
      zk_client = null
    }

    if( master!=null ) {
      val latch = new CountDownLatch(1)
      stop_master {
        latch.countDown()
      }
      latch.await()
    }
    if( slave !=null ) {
      val latch = new CountDownLatch(1)
      stop_slave {
        latch.countDown()
      }
      latch.await()

    }
    if( master_started.get() ) {
      stopped_latch.countDown()
    }
  }

  def start_slave(address: String)(func: => Unit) = {
    assert(master==null)
    slave = create_slave()
    slave.connect = address
    slave.blocking_executor.execute(^{
      slave.start();
      func
    })
  }

  def stop_slave(func: => Unit) = {
    if( slave!=null ) {
      val s = slave
      slave = null
      s.blocking_executor.execute(^{
        s.stop();
        position = s.wal_append_position
        func
      })
    }
  }

  def create_slave() = {
    val slave = new SlaveLevelDBStore();
    configure(slave)
    slave
  }

  def create_master() = {
    val master = new MasterLevelDBStore
    configure(master)
    master.replicas = replicas
    master.bind = bind
    master.syncTo = sync
    master
  }

  override def setBrokerName(brokerName: String): Unit = {
    this.brokerName = brokerName
  }


  override def deleteAllMessages {
    if(proxy_target != null) proxy_target.deleteAllMessages
    else {
      info("You instructed the broker to delete all messages (on startup?). " +
        "Cannot delete all messages from an ElectingLevelDBStore because we need to decide who the master is first")
    }
  }

  def configure(store: ReplicatedLevelDBStoreTrait) {
    store.directory = directory
    store.indexFactory = indexFactory
    store.verifyChecksums = verifyChecksums
    store.indexMaxOpenFiles = indexMaxOpenFiles
    store.indexBlockRestartInterval = indexBlockRestartInterval
    store.paranoidChecks = paranoidChecks
    store.indexWriteBufferSize = indexWriteBufferSize
    store.indexBlockSize = indexBlockSize
    store.indexCompression = indexCompression
    store.logCompression = logCompression
    store.indexCacheSize = indexCacheSize
    store.flushDelay = flushDelay
    store.asyncBufferSize = asyncBufferSize
    store.monitorStats = monitorStats
    store.securityToken = securityToken
    store.setFailoverProducersAuditDepth(failoverProducersAuditDepth)
    store.setMaxFailoverProducersToTrack(maxFailoverProducersToTrack)
    store.setBrokerName(brokerName)
    store.setBrokerService(brokerService)
    store.setUsageManager(usageManager)
  }

  def address(port: Int) = {
    if( connectUrl==null ) {
      if (hostname == null) {
        hostname = machine_hostname
      }
      "tcp://" + hostname + ":" + port
    } else {
      connectUrl;
    }
  }

  override def size: Long = {
    if( master !=null ) {
      master.size
    } else if( slave !=null ) {
      slave.size
    } else {
      var rc = 0L
      if( directory.exists() ) {
        for( f <- directory.list() ) {
          if( f.endsWith(LevelDBClient.LOG_SUFFIX)) {
            rc += f.length
          }
        }
      }
      rc
    }
  }
}


class ReplicatedLevelDBStoreView(val store:ElectingLevelDBStore) extends ReplicatedLevelDBStoreViewMBean {
  import store._

  def getZkAddress = zkAddress
  def getZkPath = zkPath
  def getZkSessionTimeout = zkSessionTimeout
  def getBind = bind
  def getReplicas = replicas

  def getNodeRole:String = {
    if( slave!=null ) {
      return "slave"
    }
    if( master!=null ) {
      return "master"
    }
    "electing"
  }

  def getStatus:String = {
    if( slave!=null ) {
      return slave.status
    }
    if( master!=null ) {
      return master.status
    }
    ""
  }

  object SlaveStatusOTF extends OpenTypeSupport.AbstractOpenTypeFactory {
    protected def getTypeName: String = classOf[SlaveStatus].getName

    protected override def init() = {
      super.init();
      addItem("nodeId", "nodeId", SimpleType.STRING);
      addItem("remoteAddress", "remoteAddress", SimpleType.STRING);
      addItem("attached", "attached", SimpleType.BOOLEAN);
      addItem("position", "position", SimpleType.LONG);
    }

    override def getFields(o: Any): util.Map[String, AnyRef] = {
      val status = o.asInstanceOf[SlaveStatus]
      val rc = super.getFields(o);
      rc.put("nodeId", status.nodeId);
      rc.put("remoteAddress", status.remoteAddress);
      rc.put("attached", status.attached.asInstanceOf[java.lang.Boolean]);
      rc.put("position", status.position.asInstanceOf[java.lang.Long]);
      rc
    }
  }

  def getSlaves():Array[CompositeData] =  {
    if( master!=null ) {
      master.slaves_status.map { status =>
        val fields = SlaveStatusOTF.getFields(status);
        new CompositeDataSupport(SlaveStatusOTF.getCompositeType(), fields).asInstanceOf[CompositeData]
      }.toArray
    } else {
      Array()
    }
  }

  def getPosition:java.lang.Long = {
    if( slave!=null ) {
      return new java.lang.Long(slave.wal_append_position)
    }
    if( master!=null ) {
      return new java.lang.Long(master.wal_append_position)
    }
    null
  }

  def getPositionDate:java.lang.Long = {
    val rc = if( slave!=null ) {
      slave.wal_date
    } else if( master!=null ) {
      master.wal_date
    } else {
      0
    }
    if( rc != 0 ) {
      return new java.lang.Long(rc)
    } else {
      return null
    }
  }

  def getDirectory = directory.getCanonicalPath
  def getSync = sync

  def getNodeId: String = node_id
}
