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

import org.apache.activemq.broker.{SuppressReplyException, LockableServiceSupport, BrokerServiceAware, ConnectionContext}
import org.apache.activemq.command._
import org.apache.activemq.openwire.OpenWireFormat
import org.apache.activemq.usage.SystemUsage
import java.io.File
import java.io.IOException
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicLong
import beans.BeanProperty
import org.apache.activemq.store._
import java.util._
import collection.mutable.ListBuffer
import org.apache.activemq.broker.jmx.{BrokerMBeanSupport, AnnotatedMBean}
import org.apache.activemq.util._
import org.apache.activemq.leveldb.util.Log
import org.apache.activemq.store.PList.PListIterator
import org.fusesource.hawtbuf.{UTF8Buffer, DataByteArrayOutputStream}
import org.fusesource.hawtdispatch;
import org.apache.activemq.broker.scheduler.JobSchedulerStore
import org.apache.activemq.store.IndexListener.MessageContext
import javax.management.ObjectName

object LevelDBStore extends Log {
  val DEFAULT_DIRECTORY = new File("LevelDB");

  lazy val BLOCKING_EXECUTOR = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 10, TimeUnit.SECONDS, new SynchronousQueue[Runnable](), new ThreadFactory() {
      def newThread(r:Runnable) = {
          val rc = new Thread(null, r, "ActiveMQ Task");
          rc.setDaemon(true);
          rc
      }
  })

  val DONE = new InlineListenableFuture;

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

  def waitOn(future: java.util.concurrent.Future[AnyRef]): Unit = {
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
  var gcPosition = 0L
  var lastAckPosition = 0L
  var cursorPosition = 0L
}

class LevelDBStoreTest(val store:LevelDBStore) extends LevelDBStoreTestMBean {

  import store._
  var suspendForce = false;

  override def setSuspendForce(value: Boolean): Unit = this.synchronized {
    if( suspendForce!=value ) {
      suspendForce = value;
      if( suspendForce ) {
        db.client.log.recordLogTestSupport.forceCall.suspend
      } else {
        db.client.log.recordLogTestSupport.forceCall.resume
      }
    }
  }

  override def getSuspendForce: Boolean = this.synchronized {
    suspendForce
  }

  override def getForceCalls = this.synchronized {
    db.client.log.recordLogTestSupport.forceCall.threads.get()
  }

  var suspendWrite = false;

  override def setSuspendWrite(value: Boolean): Unit = this.synchronized {
    if( suspendWrite!=value ) {
      suspendWrite = value;
      if( suspendWrite ) {
        db.client.log.recordLogTestSupport.writeCall.suspend
      } else {
        db.client.log.recordLogTestSupport.writeCall.resume
      }
    }
  }

  override def getSuspendWrite: Boolean = this.synchronized {
    suspendWrite
  }

  override def getWriteCalls = this.synchronized {
    db.client.log.recordLogTestSupport.writeCall.threads.get()
  }

  var suspendDelete = false;

  override def setSuspendDelete(value: Boolean): Unit = this.synchronized {
    if( suspendDelete!=value ) {
      suspendDelete = value;
      if( suspendDelete ) {
        db.client.log.recordLogTestSupport.deleteCall.suspend
      } else {
        db.client.log.recordLogTestSupport.deleteCall.resume
      }
    }
  }

  override def getSuspendDelete: Boolean = this.synchronized {
    suspendDelete
  }

  override def getDeleteCalls = this.synchronized {
    db.client.log.recordLogTestSupport.deleteCall.threads.get()
  }

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

  def compact() {
    import hawtdispatch._
    var done = new CountDownLatch(1)
    val positions = getTopicGCPositions
    client.writeExecutor {
      client.index.compact_needed = true
      client.gc(positions)
      done.countDown()
    }
    done.await()
  }
}

import LevelDBStore._

class LevelDBStore extends LockableServiceSupport with BrokerServiceAware with PersistenceAdapter with TransactionStore with PListStore with TransactionIdTransformerAware {

  final val wireFormat = new OpenWireFormat
  final val db = new DBManager(this)
  final var client = createClient

  @BeanProperty
  var directory = DEFAULT_DIRECTORY
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
  var flushDelay = 0
  @BeanProperty
  var asyncBufferSize = 1024*1024*4
  @BeanProperty
  var monitorStats = false
  @BeanProperty
  var autoCompactionRatio = 250

  var purgeOnStatup: Boolean = false

  val queues = collection.mutable.HashMap[ActiveMQQueue, LevelDBStore#LevelDBMessageStore]()
  val topics = collection.mutable.HashMap[ActiveMQTopic, LevelDBStore#LevelDBTopicMessageStore]()
  val topicsById = collection.mutable.HashMap[Long, LevelDBStore#LevelDBTopicMessageStore]()
  val plists = collection.mutable.HashMap[String, LevelDBStore#LevelDBPList]()

  private val lock = new Object();

  def check_running = {
    if( this.isStopped ) {
      throw new SuppressReplyException("Store has been stopped")
    }
  }

  def init() = {}

  def createDefaultLocker() = {
    var locker = new SharedFileLocker();
    locker.configure(this);
    locker
  }

  override def toString: String = {
    return "LevelDB[" + directory.getAbsolutePath + "]"
  }

  def objectName = {
    var brokerON = brokerService.getBrokerObjectName
    BrokerMBeanSupport.createPersistenceAdapterName(brokerON.toString, this.toString)
  }

  var snappyCompressLogs = false

  def doStart: Unit = {
    if( brokerService!=null ) {
      wireFormat.setVersion(brokerService.getStoreOpenWireVersion)
    }
    snappyCompressLogs = logCompression.toLowerCase == "snappy" && Snappy != null
    debug("starting")

    // Expose a JMX bean to expose the status of the store.
    if(brokerService!=null && brokerService.isUseJmx){
      try {
        AnnotatedMBean.registerMBean(brokerService.getManagementContext, new LevelDBStoreView(this), objectName)
        if( java.lang.Boolean.getBoolean("org.apache.activemq.leveldb.test") ) {
          val name = new ObjectName(objectName.toString + ",view=Test")
          AnnotatedMBean.registerMBean(brokerService.getManagementContext, new LevelDBStoreTest(this), name)
        }
      } catch {
        case e: Throwable => {
          warn(e, "LevelDB Store could not be registered in JMX: " + e.getMessage)
        }
      }
    }

    if (purgeOnStatup) {
      purgeOnStatup = false
      db.client.locked_purge
      info("Purged: "+this)
    }

    db.start
    db.loadCollections

    // Finish recovering the prepared XA transactions.
    import collection.JavaConversions._
    for( (txid, transaction) <- transactions ) {
      assert( transaction.xacontainer_id != -1 )
      val (msgs, acks) = db.getXAActions(transaction.xacontainer_id)
      transaction.xarecovery = (msgs, acks.map(_.ack))
      for ( msg <- msgs ) {
        transaction.add(createMessageStore(msg.getDestination), null, msg, false);
      }
      for ( record <- acks ) {
        var ack = record.ack
        var store = createMessageStore(ack.getDestination)
        if( record.sub == -1 ) {
          store.preparedAcks.add(ack.getLastMessageId)
          transaction.remove(store, ack);
        } else {
          val topic = store.asInstanceOf[LevelDBTopicMessageStore];
          for ( sub <- topic.subscription_with_key(record.sub) ) {
            val position = db.queuePosition(ack.getLastMessageId)
            transaction.updateAckPosition( topic, sub, position, ack);
            sub.lastAckPosition = position
          }
        }
      }
    }

    // Remove topics that don't have subs..
    for( (name, topic) <- topics.toArray ) {
      if( topic.subscription_count == 0 ) {
        removeTopicMessageStore(name)
      }
    }

    debug("started")
  }

  def doStop(stopper: ServiceStopper): Unit = {
    db.stop
    if(brokerService!=null && brokerService.isUseJmx){
      brokerService.getManagementContext().unregisterMBean(objectName);
      if( java.lang.Boolean.getBoolean("org.apache.activemq.leveldb.test") )
        brokerService.getManagementContext().unregisterMBean(new ObjectName(objectName.toString+",view=Test"));
    }
    info("Stopped "+this)
  }

  def broker_service = brokerService

  def blocking_executor:Executor = {
    if( broker_service != null ) {
      broker_service.getTaskRunnerFactory
    } else {
      BLOCKING_EXECUTOR
    }
  }

  var transactionIdTransformer: TransactionIdTransformer = new TransactionIdTransformer{
    def transform(txid: TransactionId): TransactionId = txid
  }

  def setTransactionIdTransformer(transactionIdTransformer: TransactionIdTransformer) {
    this.transactionIdTransformer = transactionIdTransformer
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

  def createTransactionStore = new LevelDBTransactionStore(this)

  val transactions = new ConcurrentHashMap[TransactionId, Transaction]()

  trait TransactionAction {
    def commit(uow:DelayableUOW):Unit
    def prepare(uow:DelayableUOW):Unit
    def rollback(uow:DelayableUOW):Unit
  }

  case class Transaction(id:TransactionId) {
    val commitActions = ListBuffer[TransactionAction]()

    val xaseqcounter: AtomicLong = new AtomicLong(0)
    var xarecovery:(ListBuffer[Message], ListBuffer[MessageAck]) = null
    var xacontainer_id = -1L

    def prepared = xarecovery!=null
    def prepare = {
      if( !prepared ) {
        val done = new CountDownLatch(1)
        withUow { uow =>
          xarecovery = (ListBuffer[Message](), ListBuffer[MessageAck]())
          xacontainer_id = db.createTransactionContainer(id.asInstanceOf[XATransactionId])
          for ( action <- commitActions ) {
            action.prepare(uow)
          }
          uow.syncFlag = true
          uow.addCompleteListener(done.countDown())
        }
        done.await()
      }
    }

  def add(store:LevelDBStore#LevelDBMessageStore, context: ConnectionContext, message: Message, delay:Boolean) = {
      commitActions += new TransactionAction() {
        def commit(uow:DelayableUOW) = {
          if( prepared ) {
            uow.dequeue(xacontainer_id, message.getMessageId)
          }
          var copy = message.getMessageId.copy()
          copy.setEntryLocator(null)
          message.setMessageId(copy)
          store.doAdd(uow, context, message, delay)
        }

        def prepare(uow:DelayableUOW) = {
          // add it to the xa container instead of the actual store container.
          uow.enqueue(xacontainer_id, xaseqcounter.incrementAndGet, message, delay)
          xarecovery._1 += message
        }

        def rollback(uow:DelayableUOW) = {
          if( prepared ) {
            uow.dequeue(xacontainer_id, message.getMessageId)
          }
        }

      }
    }

    def remove(store:LevelDBStore#LevelDBMessageStore, ack:MessageAck) = {
      commitActions += new TransactionAction() {

        def commit(uow:DelayableUOW) = {
          store.doRemove(uow, ack.getLastMessageId)
          if( prepared ) {
            store.preparedAcks.remove(ack.getLastMessageId)
          }
        }

        def prepare(uow:DelayableUOW) = {
          // add it to the xa container instead of the actual store container.
          uow.xaAck(XaAckRecord(xacontainer_id, xaseqcounter.incrementAndGet, ack))
          xarecovery._2 += ack
          store.preparedAcks.add(ack.getLastMessageId)
        }

        def rollback(uow: DelayableUOW) {
          if( prepared ) {
            store.preparedAcks.remove(ack.getLastMessageId)
          }
        }
      }
    }

    def updateAckPosition(store:LevelDBStore#LevelDBTopicMessageStore, sub: DurableSubscription, position: Long, ack:MessageAck) = {
      commitActions += new TransactionAction() {
        var prev_position = sub.lastAckPosition

        def commit(uow:DelayableUOW) = {
          store.doUpdateAckPosition(uow, sub, position)
          sub.gcPosition = position
        }
        def prepare(uow:DelayableUOW) = {
          prev_position = sub.lastAckPosition
          sub.lastAckPosition = position
          uow.xaAck(XaAckRecord(xacontainer_id, xaseqcounter.incrementAndGet, ack, sub.subKey))
        }
        def rollback(uow: DelayableUOW) {
          if ( prepared ) {
            sub.lastAckPosition = prev_position
          }
        }
      }
    }
  }

  def transaction(original: TransactionId) = {
    val txid = transactionIdTransformer.transform(original)
    var rc = transactions.get(txid)
    if( rc == null ) {
      rc = Transaction(txid)
      val prev = transactions.putIfAbsent(txid, rc)
      if (prev!=null) {
        rc = prev
      }
    }
    rc
  }

  def verify_running = {
    if( isStopping || isStopped ) {
      try {
        throw new IOException("Not running")
      } catch {
        case e:IOException =>
          if( broker_service!=null ) {
            broker_service.handleIOException(e)
          }
          throw new SuppressReplyException(e);
      }
    }
  }

  def commit(original: TransactionId, wasPrepared: Boolean, preCommit: Runnable, postCommit: Runnable) = {

    verify_running

    val txid = transactionIdTransformer.transform(original)
    transactions.remove(txid) match {
      case null =>
        // Only in-flight non-persistent messages in this TX.
        if( preCommit!=null )
          preCommit.run()
        if( postCommit!=null )
          postCommit.run()
      case tx =>
        val done = new CountDownLatch(1)
        // Ugly synchronization hack to make sure messages are ordered the way the cursor expects them.
        transactions.synchronized {
          withUow { uow =>
            for( action <- tx.commitActions ) {
              action.commit(uow)
            }
            uow.syncFlag = true
            uow.addCompleteListener {
              if( preCommit!=null )
                preCommit.run()
              done.countDown()
            }
          }
        }
        done.await()
        if( tx.prepared ) {
          db.removeTransactionContainer(tx.xacontainer_id)
        }
        if( postCommit!=null )
          postCommit.run()
    }
  }

  def rollback(original: TransactionId) = {
    verify_running

    val txid = transactionIdTransformer.transform(original)
    transactions.remove(txid) match {
      case null =>
        debug("on rollback, the transaction " + txid + " does not exist")
      case tx =>
        val done = new CountDownLatch(1)
        withUow { uow =>
          for( action <- tx.commitActions.reverse ) {
            action.rollback(uow)
          }
          uow.syncFlag = true
          uow.addCompleteListener { done.countDown() }
        }
        done.await()
        if( tx.prepared ) {
          db.removeTransactionContainer(tx.xacontainer_id)
        }
    }
  }

  def prepare(original: TransactionId) = {
    verify_running

    val tx = transactionIdTransformer.transform(original)
    transactions.get(tx) match {
      case null =>
        warn("on prepare, the transaction " + tx + " does not exist")
      case tx =>
        tx.prepare
    }
  }

  var doingRecover = false
  def recover(listener: TransactionRecoveryListener) = {

    verify_running

    this.doingRecover = true
    try {
      import collection.JavaConversions._
      for ( (txid, transaction) <- transactions ) {
        if( transaction.prepared ) {
          val (msgs, acks) = transaction.xarecovery
          listener.recover(txid.asInstanceOf[XATransactionId], msgs.toArray, acks.toArray);
        }
      }
    } finally {
      this.doingRecover = false
    }
  }


  def getPList(name: String): PList = {
    lock.synchronized(plists.get(name)).getOrElse(db.createPList(name))
  }

  def createPList(name: String, key: Long):LevelDBStore#LevelDBPList = {
    var rc = new LevelDBPList(name, key)
    lock.synchronized {
      plists.put(name, rc)
    }
    rc
  }

  def removePList(name: String): Boolean = {
    plists.remove(name) match {
      case Some(list)=>
        db.destroyPList(list.key)
        list.listSize.set(0)
        true
      case None =>
        false
    }
  }


  def createMessageStore(destination: ActiveMQDestination):LevelDBStore#LevelDBMessageStore = {
    destination match {
      case destination:ActiveMQQueue =>
        createQueueMessageStore(destination)
      case destination:ActiveMQTopic =>
        createTopicMessageStore(destination)
    }
  }

  def createQueueMessageStore(destination: ActiveMQQueue):LevelDBStore#LevelDBMessageStore = {
    lock.synchronized(queues.get(destination)).getOrElse(db.createQueueStore(destination))
  }

  def createQueueMessageStore(destination: ActiveMQQueue, key: Long):LevelDBStore#LevelDBMessageStore = {
    var rc = new LevelDBMessageStore(destination, key)
    lock.synchronized {
      queues.put(destination, rc)
    }
    rc
  }

  def removeQueueMessageStore(destination: ActiveMQQueue): Unit = lock synchronized {
    queues.remove(destination).foreach { store=>
      db.destroyQueueStore(store.key)
    }
  }

  def createTopicMessageStore(destination: ActiveMQTopic):LevelDBStore#LevelDBTopicMessageStore = {
    lock.synchronized(topics.get(destination)).getOrElse(db.createTopicStore(destination))
  }

  def createTopicMessageStore(destination: ActiveMQTopic, key: Long):LevelDBStore#LevelDBTopicMessageStore = {
    var rc = new LevelDBTopicMessageStore(destination, key)
    lock synchronized {
      topics.put(destination, rc)
      topicsById.put(key, rc)
    }
    rc
  }

  def createJobSchedulerStore():JobSchedulerStore = {
    throw new UnsupportedOperationException();
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

  def getLastProducerSequenceId(id: ProducerId) = db.getLastProducerSequenceId(id)

  def setMaxFailoverProducersToTrack(maxFailoverProducersToTrack:Int ) = {
      db.producerSequenceIdTracker.setMaximumNumberOfProducersToTrack(maxFailoverProducersToTrack);
  }

  def getMaxFailoverProducersToTrack() = {
    db.producerSequenceIdTracker.getMaximumNumberOfProducersToTrack()
  }

  def setFailoverProducersAuditDepth(failoverProducersAuditDepth:Int) = {
      db.producerSequenceIdTracker.setAuditDepth(failoverProducersAuditDepth);
  }

  def getFailoverProducersAuditDepth() = {
      db.producerSequenceIdTracker.getAuditDepth();
  }

  def size: Long = {
    return db.client.size
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

    val lastSeq: AtomicLong = new AtomicLong(0)
    protected var cursorPosition: Long = 0
    val preparedAcks = new HashSet[MessageId]()
    val pendingCursorAdds = new LinkedList[Long]()
    lastSeq.set(db.getLastQueueEntrySeq(key))

    def cursorResetPosition = 0L

    def doAdd(uow: DelayableUOW, context: ConnectionContext, message: Message, delay:Boolean): CountDownFuture[AnyRef] = {
      check_running
      message.beforeMarshall(wireFormat);
      message.incrementReferenceCount()
      uow.addCompleteListener({
        message.decrementReferenceCount()
      })
      lastSeq.synchronized {
        val seq = lastSeq.incrementAndGet()
        message.getMessageId.setFutureOrSequenceLong(seq);
        // null context on xa recovery, we want to bypass the cursor & pending adds as it will be reset
        if (indexListener != null && context != null) {
          pendingCursorAdds.synchronized { pendingCursorAdds.add(seq) }
          indexListener.onAdd(new MessageContext(context, message, new Runnable {
            def run(): Unit = pendingCursorAdds.synchronized { pendingCursorAdds.remove(seq) }
          }))
        }
        uow.enqueue(key, seq, message, delay)
      }
    }

    override def asyncAddQueueMessage(context: ConnectionContext, message: Message) = asyncAddQueueMessage(context, message, false)
    override def asyncAddQueueMessage(context: ConnectionContext, message: Message, delay: Boolean): ListenableFuture[AnyRef] = {
      check_running
      message.getMessageId.setEntryLocator(null)
      if(  message.getTransactionId!=null ) {
        transaction(message.getTransactionId).add(this, context, message, delay)
        DONE
      } else {
        withUow { uow=>
          doAdd(uow, context, message, delay)
        }
      }
    }

    override def addMessage(context: ConnectionContext, message: Message) = addMessage(context, message, false)
    override def addMessage(context: ConnectionContext, message: Message, delay: Boolean): Unit = {
      check_running
      waitOn(asyncAddQueueMessage(context, message, delay))
    }

    override def updateMessage(message: Message): Unit = {
      check_running
      // the only current usage of update is to increment the redelivery counter
      withUow {uow => uow.incrementRedelivery(key, message.getMessageId)}
    }

    def doRemove(uow: DelayableUOW, id: MessageId): CountDownFuture[AnyRef] = {
      uow.dequeue(key, id)
    }

    override def removeAsyncMessage(context: ConnectionContext, ack: MessageAck): Unit = {
      check_running
      if(  ack.getTransactionId!=null ) {
        transaction(ack.getTransactionId).remove(this, ack)
      } else {
        waitOn(withUow{uow=>
          doRemove(uow, ack.getLastMessageId)
        })
      }
    }

    def removeMessage(context: ConnectionContext, ack: MessageAck): Unit = {
      check_running
      removeAsyncMessage(context, ack)
    }

    def getMessage(id: MessageId): Message = {
      check_running
      var message: Message = db.getMessage(id)
      if (message == null) {
        throw new IOException("Message id not found: " + id)
      }
      return message
    }

    def removeAllMessages(context: ConnectionContext): Unit = {
      check_running
      db.collectionEmpty(key)
      cursorPosition = cursorResetPosition
    }

    override def getMessageCount: Int = {
      return db.collectionSize(key).toInt
    }

    override def isEmpty: Boolean = {
      return db.collectionIsEmpty(key)
    }

    def getCursorPendingLimit: Long = {
      pendingCursorAdds.synchronized { Option(pendingCursorAdds.peek).getOrElse(Long.MaxValue) }
    }

    def recover(listener: MessageRecoveryListener): Unit = {
      check_running
      cursorPosition = db.cursorMessages(preparedAcks, key, listener, cursorResetPosition, getCursorPendingLimit)
    }

    def resetBatching: Unit = {
      cursorPosition = cursorResetPosition
    }

    def recoverNextMessages(maxReturned: Int, listener: MessageRecoveryListener): Unit = {
      check_running
      cursorPosition = db.cursorMessages(preparedAcks, key, listener, cursorPosition, getCursorPendingLimit, maxReturned)
    }

    override def setBatch(id: MessageId): Unit = {
      cursorPosition = Math.min(getCursorPendingLimit, db.queuePosition(id)) + 1
    }

  }

  //
  // This gts called when the store is first loading up, it restores
  // the existing durable subs..
  def createSubscription(sub:DurableSubscription) = {
    lock.synchronized(topicsById.get(sub.topicKey)) match {
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
    val topics = lock.synchronized {
      new ArrayList(topicsById.values())
    }
    topics.flatMap(_.gcPosition).toSeq
  }

  class LevelDBTopicMessageStore(dest: ActiveMQDestination, key: Long) extends LevelDBMessageStore(dest, key) with TopicMessageStore {
    val subscriptions = collection.mutable.HashMap[(String, String), DurableSubscription]()
    var firstSeq = 0L

    override def cursorResetPosition = firstSeq

    def subscription_with_key(key:Long) = subscriptions.find(_._2.subKey == key).map(_._2)

    override def asyncAddQueueMessage(context: ConnectionContext, message: Message, delay: Boolean): ListenableFuture[AnyRef] = {
      super.asyncAddQueueMessage(context, message, false)
    }

    var stats = new MessageStoreSubscriptionStatistics(false)

    def getMessageStoreSubStatistics: MessageStoreSubscriptionStatistics = {
        stats;
    }

    def subscription_count = subscriptions.synchronized {
      subscriptions.size
    }

    def gcPosition:Option[(Long, Long)] = {
      var pos = lastSeq.get()
      subscriptions.synchronized {
        subscriptions.values.foreach { sub =>
          if( sub.gcPosition < pos ) {
            pos = sub.gcPosition
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

    def addSubscription(info: SubscriptionInfo, retroactive: Boolean) = {
      check_running
      var sub = db.addSubscription(key, info)
      subscriptions.synchronized {
        subscriptions.put((info.getClientId, info.getSubcriptionName), sub)
      }
      sub.lastAckPosition = if (retroactive) 0 else lastSeq.get()
      sub.gcPosition = sub.lastAckPosition
      waitOn(withUow{ uow=>
        uow.updateAckPosition(sub.subKey, sub.lastAckPosition)
        uow.countDownFuture
      })
    }

    def getAllSubscriptions: Array[SubscriptionInfo] = subscriptions.synchronized {
      check_running
      subscriptions.values.map(_.info).toArray
    }

    def lookupSubscription(clientId: String, subscriptionName: String): SubscriptionInfo = subscriptions.synchronized {
      check_running
      subscriptions.get((clientId, subscriptionName)).map(_.info).getOrElse(null)
    }

    def deleteSubscription(clientId: String, subscriptionName: String): Unit = {
      check_running
      subscriptions.synchronized {
        subscriptions.remove((clientId, subscriptionName))
      }.foreach(db.removeSubscription(_))
    }

    private def lookup(clientId: String, subscriptionName: String): Option[DurableSubscription] = subscriptions.synchronized {
      subscriptions.get((clientId, subscriptionName))
    }

    def doUpdateAckPosition(uow: DelayableUOW, sub: DurableSubscription, position: Long) = {
      sub.lastAckPosition = position
      sub.gcPosition = position
      uow.updateAckPosition(sub.subKey, sub.lastAckPosition)
    }

    def acknowledge(context: ConnectionContext, clientId: String, subscriptionName: String, messageId: MessageId, ack: MessageAck): Unit = {
      check_running
      lookup(clientId, subscriptionName).foreach { sub =>
        var position = db.queuePosition(messageId)
        if(  ack.getTransactionId!=null ) {
          transaction(ack.getTransactionId).updateAckPosition(this, sub, position, ack)
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
      check_running
      lookup(clientId, subscriptionName).foreach { sub =>
        sub.cursorPosition = 0
      }
    }
    def recoverSubscription(clientId: String, subscriptionName: String, listener: MessageRecoveryListener): Unit = {
      check_running
      lookup(clientId, subscriptionName).foreach { sub =>
        sub.cursorPosition = db.cursorMessages(preparedAcks, key, listener, sub.cursorPosition.max(sub.lastAckPosition+1))
      }
    }

    def recoverNextMessages(clientId: String, subscriptionName: String, maxReturned: Int, listener: MessageRecoveryListener): Unit = {
      check_running
      lookup(clientId, subscriptionName).foreach { sub =>
        sub.cursorPosition = db.cursorMessages(preparedAcks, key, listener, sub.cursorPosition.max(sub.lastAckPosition+1), Long.MaxValue, maxReturned)
      }
    }

    def getMessageCount(clientId: String, subscriptionName: String): Int = {
      check_running
      lookup(clientId, subscriptionName) match {
        case Some(sub) =>
          (lastSeq.get - sub.lastAckPosition).toInt
        case None => 0
      }
    }

    def getMessageSize(clientId: String, subscriptionName: String): Long = {
      check_running
      return 0
    }

  }
  class LevelDBPList(val name: String, val key: Long) extends PList {
    import LevelDBClient._

    val lastSeq = new AtomicLong(Long.MaxValue/2)
    val firstSeq = new AtomicLong(lastSeq.get+1)
    val listSize = new AtomicLong(0)

    def getName: String = name
    def destroy() = {
      check_running
      removePList(name)
    }

    def addFirst(id: String, bs: ByteSequence): AnyRef = {
      check_running
      var pos = lastSeq.decrementAndGet()
      add(pos, id, bs)
      listSize.incrementAndGet()
      new java.lang.Long(pos)
    }

    def addLast(id: String, bs: ByteSequence): AnyRef = {
      check_running
      var pos = lastSeq.incrementAndGet()
      add(pos, id, bs)
      listSize.incrementAndGet()
      new java.lang.Long(pos)
    }

    def add(pos:Long, id: String, bs: ByteSequence) = {
      check_running
      val encoded_key = encodeLongLong(key, pos)
      val encoded_id = new UTF8Buffer(id)
      val os = new DataByteArrayOutputStream(2+encoded_id.length+bs.length)
      os.writeShort(encoded_id.length)
      os.write(encoded_id.data, encoded_id.offset, encoded_id.length)
      os.write(bs.getData, bs.getOffset, bs.getLength)
      db.plistPut(encoded_key, os.toBuffer.toByteArray)
    }

    def remove(position: AnyRef): Boolean = {
      check_running
      val pos = position.asInstanceOf[java.lang.Long].longValue()
      val encoded_key = encodeLongLong(key, pos)
      db.plistGet(encoded_key) match {
        case Some(value) =>
          db.plistDelete(encoded_key)
          listSize.decrementAndGet()
          true
        case None =>
          false
      }
    }

    def isEmpty = size()==0
    def size(): Long = listSize.get()
    def messageSize(): Long = 0

    def iterator() = new PListIterator() {
      check_running
      val prefix = LevelDBClient.encodeLong(key)
      var dbi = db.plistIterator
      var last_key:Array[Byte] = _

      dbi.seek(prefix);


      def hasNext: Boolean = dbi!=null && dbi.hasNext && dbi.peekNext.getKey.startsWith(prefix)
      def next() = {
        if ( dbi==null || !dbi.hasNext ) {
          throw new NoSuchElementException();
        }
        val n = dbi.peekNext();
        last_key = n.getKey
        val (k, pos) = decodeLongLong(last_key)
        if( k!=key ) {
          throw new NoSuchElementException();
        }
        var value = n.getValue
        val is = new org.fusesource.hawtbuf.DataByteArrayInputStream(value)
        val id = is.readBuffer(is.readShort()).utf8().toString
        val data = new ByteSequence(value, is.getPos, value.length-is.getPos)
        dbi.next()
        new PListEntry(id, data, pos)
      }

      def release() = {
        dbi.close()
        dbi = null
      }

      def remove() = {
        if( last_key==null ) {
          throw new NoSuchElementException();
        }
        db.plistDelete(last_key)
        listSize.decrementAndGet()
        last_key = null
      }
    }

  }

  class LevelDBTransactionStore(val store:LevelDBStore) extends TransactionStore {
    def start() = {}

    def stop() = {}

    def prepare(txid: TransactionId) = store.prepare(txid)

    def commit(txid: TransactionId, wasPrepared: Boolean, preCommit: Runnable, postCommit: Runnable) = store.commit(txid, wasPrepared, preCommit, postCommit)

    def rollback(txid: TransactionId) = store.rollback(txid)

    def recover(listener: TransactionRecoveryListener) = store.recover(listener)
  }

  ///////////////////////////////////////////////////////////////////////////
  // The following methods actually have nothing to do with JMS txs... It's more like
  // operation batch.. we handle that in the DBManager tho..
  ///////////////////////////////////////////////////////////////////////////
  def beginTransaction(context: ConnectionContext): Unit = {}
  def commitTransaction(context: ConnectionContext): Unit = {}
  def rollbackTransaction(context: ConnectionContext): Unit = {}

  def createClient = new LevelDBClient(this);

  def allowIOResumption() = {}
}
