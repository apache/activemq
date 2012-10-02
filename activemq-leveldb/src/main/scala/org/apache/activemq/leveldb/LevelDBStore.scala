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

import org.apache.activemq.broker.BrokerService
import org.apache.activemq.broker.BrokerServiceAware
import org.apache.activemq.broker.ConnectionContext
import org.apache.activemq.command._
import org.apache.activemq.openwire.OpenWireFormat
import org.apache.activemq.usage.SystemUsage
import java.io.File
import java.io.IOException
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future
import java.util.concurrent.atomic.AtomicLong
import reflect.BeanProperty
import org.apache.activemq.store._
import java.util._
import scala.collection.mutable.ListBuffer
import javax.management.ObjectName
import org.apache.activemq.broker.jmx.AnnotatedMBean
import org.apache.activemq.util._
import org.apache.kahadb.util.LockFile
import org.apache.activemq.leveldb.util.{RetrySupport, FileSupport, Log}

object LevelDBStore extends Log {
  
  val DONE = new CountDownFuture();
  DONE.countDown
  
  def toIOException(e: Throwable): IOException = {
    if (e.isInstanceOf[ExecutionException]) {
      var cause: Throwable = (e.asInstanceOf[ExecutionException]).getCause
      if (cause.isInstanceOf[IOException]) {
        return cause.asInstanceOf[IOException]
      }
    }
    if (e.isInstanceOf[IOException]) {
      return e.asInstanceOf[IOException]
    }
    return IOExceptionSupport.create(e)
  }

  def waitOn(future: Future[AnyRef]): Unit = {
    try {
      future.get
    }
    catch {
      case e: Throwable => {
        throw toIOException(e)
      }
    }
  }
}

case class DurableSubscription(subKey:Long, topicKey:Long, info: SubscriptionInfo) {
  var lastAckPosition = 0L
  var cursorPosition = 0L
}

class LevelDBStoreView(val store:LevelDBStore) extends LevelDBStoreViewMBean {
  import store._

  def getAsyncBufferSize = asyncBufferSize
  def getIndexDirectory = directory.getCanonicalPath
  def getLogDirectory = Option(logDirectory).getOrElse(directory).getCanonicalPath
  def getIndexBlockRestartInterval = indexBlockRestartInterval
  def getIndexBlockSize = indexBlockSize
  def getIndexCacheSize = indexCacheSize
  def getIndexCompression = indexCompression
  def getIndexFactory = db.client.factory.getClass.getName
  def getIndexMaxOpenFiles = indexMaxOpenFiles
  def getIndexWriteBufferSize = indexWriteBufferSize
  def getLogSize = logSize
  def getParanoidChecks = paranoidChecks
  def getSync = sync
  def getVerifyChecksums = verifyChecksums

  def getUowClosedCounter = db.uowClosedCounter
  def getUowCanceledCounter = db.uowCanceledCounter
  def getUowStoringCounter = db.uowStoringCounter
  def getUowStoredCounter = db.uowStoredCounter

  def getUowMaxCompleteLatency = db.uow_complete_latency.get
  def getMaxIndexWriteLatency = db.client.max_index_write_latency.get
  def getMaxLogWriteLatency = db.client.log.max_log_write_latency.get
  def getMaxLogFlushLatency = db.client.log.max_log_flush_latency.get
  def getMaxLogRotateLatency = db.client.log.max_log_rotate_latency.get

  def resetUowMaxCompleteLatency = db.uow_complete_latency.reset
  def resetMaxIndexWriteLatency = db.client.max_index_write_latency.reset
  def resetMaxLogWriteLatency = db.client.log.max_log_write_latency.reset
  def resetMaxLogFlushLatency = db.client.log.max_log_flush_latency.reset
  def resetMaxLogRotateLatency = db.client.log.max_log_rotate_latency.reset

  def getIndexStats = db.client.index.getProperty("leveldb.stats")
}

import LevelDBStore._

class LevelDBStore extends ServiceSupport with BrokerServiceAware with PersistenceAdapter with TransactionStore {

  final val wireFormat = new OpenWireFormat
  final val db = new DBManager(this)

  @BeanProperty
  var directory: File = null
  @BeanProperty
  var logDirectory: File = null
  
  @BeanProperty
  var logSize: Long = 1024 * 1024 * 100
  @BeanProperty
  var indexFactory: String = "org.fusesource.leveldbjni.JniDBFactory, org.iq80.leveldb.impl.Iq80DBFactory"
  @BeanProperty
  var sync: Boolean = true
  @BeanProperty
  var verifyChecksums: Boolean = false
  @BeanProperty
  var indexMaxOpenFiles: Int = 1000
  @BeanProperty
  var indexBlockRestartInterval: Int = 16
  @BeanProperty
  var paranoidChecks: Boolean = false
  @BeanProperty
  var indexWriteBufferSize: Int = 1024*1024*6
  @BeanProperty
  var indexBlockSize: Int = 4 * 1024
  @BeanProperty
  var indexCompression: String = "snappy"
  @BeanProperty
  var logCompression: String = "none"
  @BeanProperty
  var indexCacheSize: Long = 1024 * 1024 * 256L
  @BeanProperty
  var flushDelay = 1000*5
  @BeanProperty
  var asyncBufferSize = 1024*1024*4
  @BeanProperty
  var monitorStats = false
  @BeanProperty
  var failIfLocked = false

  var purgeOnStatup: Boolean = false
  var brokerService: BrokerService = null

  val queues = collection.mutable.HashMap[ActiveMQQueue, LevelDBStore#LevelDBMessageStore]()
  val topics = collection.mutable.HashMap[ActiveMQTopic, LevelDBStore#LevelDBTopicMessageStore]()
  val topicsById = collection.mutable.HashMap[Long, LevelDBStore#LevelDBTopicMessageStore]()

  override def toString: String = {
    return "LevelDB:[" + directory.getAbsolutePath + "]"
  }

  def objectName = {
    var brokerON = brokerService.getBrokerObjectName
    val broker_name = brokerON.getKeyPropertyList().get("BrokerName")
    new ObjectName(brokerON.getDomain() + ":" +
            "BrokerName="+JMXSupport.encodeObjectNamePart(broker_name)+ "," +
            "Type=LevelDBStore");
  }

  def retry[T](func : =>T):T = RetrySupport.retry(LevelDBStore, isStarted, func _)

  var lock_file: LockFile = _

  var snappyCompressLogs = false

  def doStart: Unit = {
    import FileSupport._

    snappyCompressLogs = logCompression.toLowerCase == "snappy" && Snappy != null
    debug("starting")
    if ( lock_file==null ) {
      lock_file = new LockFile(directory / "lock", true)
    }

    // Expose a JMX bean to expose the status of the store.
    if(brokerService!=null){
      try {
        AnnotatedMBean.registerMBean(brokerService.getManagementContext, new LevelDBStoreView(this), objectName)
      } catch {
        case e: Throwable => {
          warn(e, "LevelDB Store could not be registered in JMX: " + e.getMessage)
        }
      }
    }

    if (failIfLocked) {
      lock_file.lock()
    } else {
      retry {
        lock_file.lock()
      }
    }

    if (purgeOnStatup) {
      purgeOnStatup = false
      db.client.locked_purge
      info("Purged: "+this)
    }

    db.start
    db.loadCollections
    debug("started")
  }

  def doStop(stopper: ServiceStopper): Unit = {
    db.stop
    lock_file.unlock()
    if(brokerService!=null){
      brokerService.getManagementContext().unregisterMBean(objectName);
    }
    info("Stopped "+this)
  }

  def setBrokerService(brokerService: BrokerService): Unit = {
    this.brokerService = brokerService
  }

  def setBrokerName(brokerName: String): Unit = {
  }

  def setUsageManager(usageManager: SystemUsage): Unit = {
  }

  def deleteAllMessages: Unit = {
    purgeOnStatup = true
  }

  def getLastMessageBrokerSequenceId: Long = {
    return 0
  }

  def createTransactionStore = this

  val transactions = collection.mutable.HashMap[TransactionId, Transaction]()
  
  trait TransactionAction {
    def apply(uow:DelayableUOW):Unit
  }
  
  case class Transaction(id:TransactionId) {
    val commitActions = ListBuffer[TransactionAction]() 
    def add(store:LevelDBMessageStore, message: Message, delay:Boolean) = {
      commitActions += new TransactionAction() {
        def apply(uow:DelayableUOW) = {
          store.doAdd(uow, message, delay)
        }
      }
    }
    def remove(store:LevelDBMessageStore, msgid:MessageId) = {
      commitActions += new TransactionAction() {
        def apply(uow:DelayableUOW) = {
          store.doRemove(uow, msgid)
        }
      }
    }
    def updateAckPosition(store:LevelDBTopicMessageStore, sub: DurableSubscription, position: Long) = {
      commitActions += new TransactionAction() {
        def apply(uow:DelayableUOW) = {
          store.doUpdateAckPosition(uow, sub, position)
        }
      }
    }
  }
  
  def transaction(txid: TransactionId) = transactions.getOrElseUpdate(txid, Transaction(txid))
  
  def commit(txid: TransactionId, wasPrepared: Boolean, preCommit: Runnable, postCommit: Runnable) = {
    preCommit.run()
    transactions.remove(txid) match {
      case None=>
        println("The transaction does not exist")
        postCommit.run()
      case Some(tx)=>
        withUow { uow =>
          for( action <- tx.commitActions ) {
            action(uow)
          }
          uow.addCompleteListener( postCommit.run() )
        }
    }
  }

  def rollback(txid: TransactionId) = {
    transactions.remove(txid) match {
      case None=>
        println("The transaction does not exist")
      case Some(tx)=>
    }
  }

  def prepare(tx: TransactionId) = {
    sys.error("XA transactions not yet supported.")
  }
  def recover(listener: TransactionRecoveryListener) = {
  }

  def createQueueMessageStore(destination: ActiveMQQueue) = {
    this.synchronized(queues.get(destination)).getOrElse(db.createQueueStore(destination))
  }

  def createQueueMessageStore(destination: ActiveMQQueue, key: Long):LevelDBMessageStore = {
    var rc = new LevelDBMessageStore(destination, key)
    this.synchronized {
      queues.put(destination, rc)
    }
    rc
  }

  def removeQueueMessageStore(destination: ActiveMQQueue): Unit = this synchronized {
    queues.remove(destination).foreach { store=>
      db.destroyQueueStore(store.key)
    }
  }

  def createTopicMessageStore(destination: ActiveMQTopic): TopicMessageStore = {
    this.synchronized(topics.get(destination)).getOrElse(db.createTopicStore(destination))
  }

  def createTopicMessageStore(destination: ActiveMQTopic, key: Long):LevelDBTopicMessageStore = {
    var rc = new LevelDBTopicMessageStore(destination, key)
    this synchronized {
      topics.put(destination, rc)
      topicsById.put(key, rc)
    }
    rc
  }

  def removeTopicMessageStore(destination: ActiveMQTopic): Unit = {
    topics.remove(destination).foreach { store=>
      store.subscriptions.values.foreach { sub =>
        db.removeSubscription(sub)
      }
      store.subscriptions.clear()
      db.destroyQueueStore(store.key)
    }
  }

  def getLogAppendPosition = db.getLogAppendPosition

  def getDestinations: Set[ActiveMQDestination] = {
    import collection.JavaConversions._
    var rc: HashSet[ActiveMQDestination] = new HashSet[ActiveMQDestination]
    rc.addAll(topics.keys)
    rc.addAll(queues.keys)
    return rc
  }

  def getLastProducerSequenceId(id: ProducerId): Long = {
    return -1
  }

  def size: Long = {
    return 0
  }

  def checkpoint(sync: Boolean): Unit = db.checkpoint(sync)

  def withUow[T](func:(DelayableUOW)=>T):T = {
    val uow = db.createUow
    try {
      func(uow)
    } finally {
      uow.release()
    }
  }

  private def subscriptionKey(clientId: String, subscriptionName: String): String = {
    return clientId + ":" + subscriptionName
  }

  case class LevelDBMessageStore(dest: ActiveMQDestination, val key: Long) extends AbstractMessageStore(dest) {

    protected val lastSeq: AtomicLong = new AtomicLong(0)
    protected var cursorPosition: Long = 0

    lastSeq.set(db.getLastQueueEntrySeq(key))

    def doAdd(uow: DelayableUOW, message: Message, delay:Boolean): CountDownFuture = {
      uow.enqueue(key, lastSeq.incrementAndGet, message, delay)
    }


    override def asyncAddQueueMessage(context: ConnectionContext, message: Message) = asyncAddQueueMessage(context, message, false)
    override def asyncAddQueueMessage(context: ConnectionContext, message: Message, delay: Boolean): Future[AnyRef] = {
      if(  message.getTransactionId!=null ) {
        transaction(message.getTransactionId).add(this, message, delay)
        DONE
      } else {
        withUow { uow=>
          doAdd(uow, message, delay)
        }
      }
    }

    override def addMessage(context: ConnectionContext, message: Message) = addMessage(context, message, false)
    override def addMessage(context: ConnectionContext, message: Message, delay: Boolean): Unit = {
      waitOn(asyncAddQueueMessage(context, message, delay))
    }

    def doRemove(uow: DelayableUOW, id: MessageId): CountDownFuture = {
      uow.dequeue(key, id)
    }

    override def removeAsyncMessage(context: ConnectionContext, ack: MessageAck): Unit = {
      if(  ack.getTransactionId!=null ) {
        transaction(ack.getTransactionId).remove(this, ack.getLastMessageId)
        DONE
      } else {
        waitOn(withUow{uow=>
          doRemove(uow, ack.getLastMessageId)
        })
      }
    }

    def removeMessage(context: ConnectionContext, ack: MessageAck): Unit = {
      removeAsyncMessage(context, ack)
    }

    def getMessage(id: MessageId): Message = {
      var message: Message = db.getMessage(id)
      if (message == null) {
        throw new IOException("Message id not found: " + id)
      }
      return message
    }

    def removeAllMessages(context: ConnectionContext): Unit = {
      db.collectionEmpty(key)
      cursorPosition = 0
    }

    def getMessageCount: Int = {
      return db.collectionSize(key).toInt
    }

    override def isEmpty: Boolean = {
      return db.collectionIsEmpty(key)
    }

    def recover(listener: MessageRecoveryListener): Unit = {
      cursorPosition = db.cursorMessages(key, listener, 0)
    }

    def resetBatching: Unit = {
      cursorPosition = 0
    }

    def recoverNextMessages(maxReturned: Int, listener: MessageRecoveryListener): Unit = {
      cursorPosition = db.cursorMessages(key, LimitingRecoveryListener(maxReturned, listener), cursorPosition)
    }

    override def setBatch(id: MessageId): Unit = {
      cursorPosition = db.queuePosition(id)
    }

  }

  case class LimitingRecoveryListener(max: Int, listener: MessageRecoveryListener) extends MessageRecoveryListener {
    private var recovered: Int = 0
    def hasSpace = recovered < max && listener.hasSpace
    def recoverMessage(message: Message) = {
      recovered += 1;
      listener.recoverMessage(message)
    }
    def recoverMessageReference(ref: MessageId) = {
      recovered += 1;
      listener.recoverMessageReference(ref)
    }
    def isDuplicate(ref: MessageId) = listener.isDuplicate(ref)
  }
  

  //
  // This gts called when the store is first loading up, it restores
  // the existing durable subs..
  def createSubscription(sub:DurableSubscription) = {
    this.synchronized(topicsById.get(sub.topicKey)) match {
      case Some(topic) =>
        topic.synchronized {
          topic.subscriptions.put((sub.info.getClientId, sub.info.getSubcriptionName), sub)
        }
      case None =>
        // Topic does not exist.. so kill the durable sub..
        db.removeSubscription(sub)
    }
  }
  
  
  def getTopicGCPositions = {
    import collection.JavaConversions._
    val topics = this.synchronized {
      new ArrayList(topicsById.values())
    }
    topics.flatMap(_.gcPosition).toSeq
  }

  class LevelDBTopicMessageStore(dest: ActiveMQDestination, key: Long) extends LevelDBMessageStore(dest, key) with TopicMessageStore {
    val subscriptions = collection.mutable.HashMap[(String, String), DurableSubscription]()
    var firstSeq = 0L

    def gcPosition:Option[(Long, Long)] = {
      var pos = lastSeq.get()
      subscriptions.synchronized {
        subscriptions.values.foreach { sub =>
          if( sub.lastAckPosition < pos ) {
            pos = sub.lastAckPosition
          }
        }
        if( firstSeq != pos+1) {
          firstSeq = pos+1
          Some(key, firstSeq)
        } else {
          None
        }
      }
    }
    
    def addSubsciption(info: SubscriptionInfo, retroactive: Boolean) = {
      var sub = db.addSubscription(key, info)
      subscriptions.synchronized {
        subscriptions.put((info.getClientId, info.getSubcriptionName), sub)
      }
      sub.lastAckPosition = if (retroactive) 0 else lastSeq.get()
      waitOn(withUow{ uow=>
        uow.updateAckPosition(sub)
        uow.countDownFuture
      })
    }
    
    def getAllSubscriptions: Array[SubscriptionInfo] = subscriptions.synchronized {
      subscriptions.values.map(_.info).toArray
    }

    def lookupSubscription(clientId: String, subscriptionName: String): SubscriptionInfo = subscriptions.synchronized {
      subscriptions.get((clientId, subscriptionName)).map(_.info).getOrElse(null)
    }

    def deleteSubscription(clientId: String, subscriptionName: String): Unit = {
      subscriptions.synchronized {
        subscriptions.remove((clientId, subscriptionName))
      }.foreach(db.removeSubscription(_))
    }

    private def lookup(clientId: String, subscriptionName: String): Option[DurableSubscription] = subscriptions.synchronized {
      subscriptions.get((clientId, subscriptionName))
    }

    def doUpdateAckPosition(uow: DelayableUOW, sub: DurableSubscription, position: Long) = {
      sub.lastAckPosition = position
      uow.updateAckPosition(sub)
    }

    def acknowledge(context: ConnectionContext, clientId: String, subscriptionName: String, messageId: MessageId, ack: MessageAck): Unit = {
      lookup(clientId, subscriptionName).foreach { sub =>
        var position = db.queuePosition(messageId)
        if(  ack.getTransactionId!=null ) {
          transaction(ack.getTransactionId).updateAckPosition(this, sub, position)
          DONE
        } else {
          waitOn(withUow{ uow=>
            doUpdateAckPosition(uow, sub, position)
            uow.countDownFuture
          })
        }

      }
    }
    
    def resetBatching(clientId: String, subscriptionName: String): Unit = {
      lookup(clientId, subscriptionName).foreach { sub =>
        sub.cursorPosition = 0
      }
    }
    def recoverSubscription(clientId: String, subscriptionName: String, listener: MessageRecoveryListener): Unit = {
      lookup(clientId, subscriptionName).foreach { sub =>
        sub.cursorPosition = db.cursorMessages(key, listener, sub.cursorPosition.max(sub.lastAckPosition+1))
      }
    }
    
    def recoverNextMessages(clientId: String, subscriptionName: String, maxReturned: Int, listener: MessageRecoveryListener): Unit = {
      lookup(clientId, subscriptionName).foreach { sub =>
        sub.cursorPosition = db.cursorMessages(key,  LimitingRecoveryListener(maxReturned, listener), sub.cursorPosition.max(sub.lastAckPosition+1))
      }
    }
    
    def getMessageCount(clientId: String, subscriptionName: String): Int = {
      lookup(clientId, subscriptionName) match {
        case Some(sub) => (lastSeq.get - sub.lastAckPosition).toInt
        case None => 0
      }
    }

  }

  ///////////////////////////////////////////////////////////////////////////
  // The following methods actually have nothing to do with JMS txs... It's more like
  // operation batch.. we handle that in the DBManager tho.. 
  ///////////////////////////////////////////////////////////////////////////
  def beginTransaction(context: ConnectionContext): Unit = {}
  def commitTransaction(context: ConnectionContext): Unit = {}
  def rollbackTransaction(context: ConnectionContext): Unit = {}

  def createClient = new LevelDBClient(this);
}
