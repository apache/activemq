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

import org.fusesource.hawtdispatch._
import org.fusesource.hawtdispatch.BaseRetained
import java.util.concurrent._
import atomic._
import org.fusesource.hawtbuf.Buffer
import org.apache.activemq.store.{ListenableFuture, MessageRecoveryListener}
import java.lang.ref.WeakReference
import scala.Option._
import org.fusesource.hawtbuf.Buffer._
import org.apache.activemq.command._
import org.apache.activemq.leveldb.record.{SubscriptionRecord, CollectionRecord}
import java.util.HashMap
import collection.mutable.{HashSet, ListBuffer}
import org.apache.activemq.util.ByteSequence
import util.TimeMetric
import scala.Some
import org.apache.activemq.ActiveMQMessageAuditNoSync
import org.fusesource.hawtdispatch
import org.apache.activemq.broker.SuppressReplyException

case class EntryLocator(qid:Long, seq:Long)
case class DataLocator(store:LevelDBStore, pos:Long, len:Int) {
  override def toString: String = "DataLocator(%x, %d)".format(pos, len)
}
case class MessageRecord(store:LevelDBStore, id:MessageId, data:Buffer, syncNeeded:Boolean) {
  var locator:DataLocator = _
}
case class QueueEntryRecord(id:MessageId, queueKey:Long, queueSeq:Long, deliveries:Int=0)
case class QueueRecord(id:ActiveMQDestination, queue_key:Long)
case class QueueEntryRange()
case class SubAckRecord(subKey:Long, ackPosition:Long)
case class XaAckRecord(container:Long, seq:Long, ack:MessageAck, sub:Long = -1)

sealed trait UowState {
  def stage:Int
}
// UoW is initial open.
object UowOpen extends UowState {
  override def stage = 0
  override def toString = "UowOpen"
}
// UoW is Committed once the broker finished creating it.
object UowClosed extends UowState {
  override def stage = 1
  override def toString = "UowClosed"
}
// UOW is delayed until we send it to get flushed.
object UowDelayed extends UowState {
  override def stage = 2
  override def toString = "UowDelayed"
}
object UowFlushQueued extends UowState {
  override def stage = 3
  override def toString = "UowFlushQueued"
}

object UowFlushing extends UowState {
  override def stage = 4
  override def toString = "UowFlushing"
}
// Then it moves on to be flushed. Flushed just
// means the message has been written to disk
// and out of memory
object UowFlushed extends UowState {
  override def stage = 5
  override def toString = "UowFlushed"
}

// Once completed then you know it has been synced to disk.
object UowCompleted extends UowState {
  override def stage = 6
  override def toString = "UowCompleted"
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class CountDownFuture[T <: AnyRef]() extends ListenableFuture[T] {

  private val latch:CountDownLatch=new CountDownLatch(1)
  @volatile
  var value:T = _
  var error:Throwable = _
  var listener:Runnable = _

  def cancel(mayInterruptIfRunning: Boolean) = false
  def isCancelled = false


  def completed = latch.getCount()==0
  def await() = latch.await()
  def await(p1: Long, p2: TimeUnit) = latch.await(p1, p2)

  def set(v:T) = {
    value = v
    latch.countDown()
    fireListener
  }
  def failed(v:Throwable) = {
    error = v
    latch.countDown()
    fireListener
  }

  def get() = {
    latch.await()
    if( error!=null ) {
      throw error;
    }
    value
  }

  def get(p1: Long, p2: TimeUnit) = {
    if(latch.await(p1, p2)) {
      if( error!=null ) {
        throw error;
      }
      value
    } else {
      throw new TimeoutException
    }
  }

  def isDone = latch.await(0, TimeUnit.SECONDS);

  def fireListener = {
    if (listener != null) {
      try {
        listener.run()
      } catch {
        case e : Throwable => {
          LevelDBStore.warn(e, "unexpected exception on future listener " +listener)
        }
      }
    }
  }

  def addListener(l: Runnable) = {
    listener = l
    if (isDone) {
      fireListener
    }
  }
}

object UowManagerConstants {
  val QUEUE_COLLECTION_TYPE = 1
  val TOPIC_COLLECTION_TYPE = 2
  val TRANSACTION_COLLECTION_TYPE = 3
  val SUBSCRIPTION_COLLECTION_TYPE = 4

  case class QueueEntryKey(queue:Long, seq:Long)
  def key(x:QueueEntryRecord) = QueueEntryKey(x.queueKey, x.queueSeq)
}

import UowManagerConstants._

class DelayableUOW(val manager:DBManager) extends BaseRetained {
  val countDownFuture = new CountDownFuture[AnyRef]()
  var canceled = false;

  val uowId:Int = manager.lastUowId.incrementAndGet()
  var actions = Map[MessageId, MessageAction]()
  var subAcks = ListBuffer[SubAckRecord]()
  var completed = false
  var disableDelay = false
  var delayableActions = 0

  private var _state:UowState = UowOpen

  def state = this._state
  def state_=(next:UowState) {
    assert(this._state.stage < next.stage)
    this._state = next
  }

  var syncFlag = false
  def syncNeeded = syncFlag || actions.find( _._2.syncNeeded ).isDefined
  def size = 100+actions.foldLeft(0L){ case (sum, entry) =>
    sum + (entry._2.size+100)
  } + (subAcks.size * 100)

  class MessageAction {
    var id:MessageId = _
    var messageRecord: MessageRecord = null
    var enqueues = ListBuffer[QueueEntryRecord]()
    var dequeues = ListBuffer[QueueEntryRecord]()
    var xaAcks = ListBuffer[XaAckRecord]()

    def uow = DelayableUOW.this
    def isEmpty() = messageRecord==null && enqueues.isEmpty && dequeues.isEmpty && xaAcks.isEmpty

    def cancel() = {
      uow.rm(id)
    }

    def syncNeeded = messageRecord!=null && messageRecord.syncNeeded
    def size = (if(messageRecord!=null) messageRecord.data.length+20 else 0) + ((enqueues.size+dequeues.size)*50) + xaAcks.foldLeft(0L){ case (sum, entry) =>
      sum + 100
    }

    def addToPendingStore() = {
      var set = manager.pendingStores.get(id)
      if(set==null) {
        set = HashSet()
        manager.pendingStores.put(id, set)
      }
      set.add(this)
    }

    def removeFromPendingStore() = {
      var set = manager.pendingStores.get(id)
      if(set!=null) {
        set.remove(this)
        if(set.isEmpty) {
          manager.pendingStores.remove(id)
        }
      }
    }

  }

  def completeAsap() = this.synchronized { disableDelay=true }
  def delayable = !disableDelay && delayableActions>0 && manager.flushDelay>0

  def rm(msg:MessageId) = {
    actions -= msg
    if( actions.isEmpty && state.stage < UowFlushing.stage ) {
      cancel
    }
  }

  def cancel = {
    manager.dispatchQueue.assertExecuting()
    manager.uowCanceledCounter += 1
    canceled = true
    manager.flush_queue.remove(uowId)
    onCompleted()
  }

  def getAction(id:MessageId) = {
    actions.get(id) match {
      case Some(x) => x
      case None =>
        val x = new MessageAction
        x.id = id
        actions += id->x
        x
    }
  }

  def updateAckPosition(sub_key:Long, ack_seq:Long) = {
    subAcks += SubAckRecord(sub_key, ack_seq)
  }

  def xaAck(record:XaAckRecord) = {
    this.synchronized {
      getAction(record.ack.getLastMessageId).xaAcks+=record
    }
    countDownFuture
  }

  def enqueue(queueKey:Long, queueSeq:Long, message:Message, delay_enqueue:Boolean)  = {
    var delay = delay_enqueue && message.getTransactionId==null
    if(delay ) {
      manager.uowEnqueueDelayReqested += 1
    } else {
      manager.uowEnqueueNodelayReqested += 1
    }

    val id = message.getMessageId

    def create_message_record: MessageRecord = {
      // encodes body and release object bodies, in case message was sent from
      // a VM connection.  Releases additional memory.
      message.storeContentAndClear()
      var packet = manager.parent.wireFormat.marshal(message)
      var data = new Buffer(packet.data, packet.offset, packet.length)
      if (manager.snappyCompressLogs) {
        data = Snappy.compress(data)
      }
      val record = MessageRecord(manager.parent, id, data, message.isResponseRequired)
      id.setDataLocator(record)
      record
    }

    val messageRecord = id.getDataLocator match {
      case null =>
        create_message_record
      case record:MessageRecord =>
        if( record.store == manager.parent ) {
          record
        } else {
          create_message_record
        }
      case x:DataLocator =>
        if( x.store == manager.parent ) {
          null
        } else {
          create_message_record
        }
    }

    val entry = QueueEntryRecord(id, queueKey, queueSeq)
    assert(id.getEntryLocator == null)
    id.setEntryLocator(EntryLocator(queueKey, queueSeq))

    val a = this.synchronized {
      if( !delay )
        disableDelay = true

      val action = getAction(entry.id)
      action.messageRecord = messageRecord
      action.enqueues += entry
      delayableActions += 1
      action
    }

    manager.dispatchQueue {
      manager.cancelable_enqueue_actions.put(key(entry), a)
      a.addToPendingStore()
    }
    countDownFuture
  }

  def incrementRedelivery(expectedQueueKey:Long, id:MessageId) = {
    if( id.getEntryLocator != null ) {
      val EntryLocator(queueKey, queueSeq) = id.getEntryLocator.asInstanceOf[EntryLocator];
      assert(queueKey == expectedQueueKey)
      val counter = manager.client.getDeliveryCounter(queueKey, queueSeq)
      val entry = QueueEntryRecord(id, queueKey, queueSeq, counter+1)
      val a = this.synchronized {
        val action = getAction(entry.id)
        action.enqueues += entry
        delayableActions += 1
        action
      }
      manager.dispatchQueue {
        manager.cancelable_enqueue_actions.put(key(entry), a)
        a.addToPendingStore()
      }
    }
    countDownFuture
  }

  def dequeue(expectedQueueKey:Long, id:MessageId) = {
    if( id.getEntryLocator != null ) {
      val EntryLocator(queueKey, queueSeq) = id.getEntryLocator.asInstanceOf[EntryLocator];
      assert(queueKey == expectedQueueKey)
      val entry = QueueEntryRecord(id, queueKey, queueSeq)
      this.synchronized {
        getAction(id).dequeues += entry
      }
    }
    countDownFuture
  }

  def complete_asap = this.synchronized {
    disableDelay=true
    if( state eq UowDelayed ) {
      manager.enqueueFlush(this)
    }
  }

  var complete_listeners = ListBuffer[()=>Unit]()
  def addCompleteListener(func: =>Unit) = {
    complete_listeners.append( func _ )
  }

  var asyncCapacityUsed = 0L
  var disposed_at = 0L

  override def dispose = this.synchronized {
    state = UowClosed
    disposed_at = System.nanoTime()
    if( !syncNeeded ) {
      val s = size
      if( manager.asyncCapacityRemaining.addAndGet(-s) > 0 ) {
        asyncCapacityUsed = s
        complete_listeners.foreach(_())
      } else {
        manager.asyncCapacityRemaining.addAndGet(s)
      }
    }
    //      closeSource.merge(this)
    manager.dispatchQueue {
      manager.processClosed(this)
    }
  }

  def onCompleted(error:Throwable=null) = this.synchronized {
    if ( state.stage < UowCompleted.stage ) {
      state = UowCompleted
      if( asyncCapacityUsed != 0 ) {
        manager.asyncCapacityRemaining.addAndGet(asyncCapacityUsed)
        asyncCapacityUsed = 0
      } else {
        manager.uow_complete_latency.add(System.nanoTime() - disposed_at)
        complete_listeners.foreach(_())
      }
      if( error == null ) {
        countDownFuture.set(null)
      } else {
        countDownFuture.failed(error)
      }

      for( (id, action) <- actions ) {
        if( !action.enqueues.isEmpty ) {
          action.removeFromPendingStore()
        }
        for( queueEntry <- action.enqueues ) {
          manager.cancelable_enqueue_actions.remove(key(queueEntry))
        }
      }
      super.dispose
    }
  }
}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class DBManager(val parent:LevelDBStore) {

  var lastCollectionKey = new AtomicLong(0)
  var lastPListKey = new AtomicLong(0)
  def client = parent.client

  def writeExecutor = client.writeExecutor
  def flushDelay = parent.flushDelay

  val dispatchQueue = createQueue(toString)
//  val aggregator = new AggregatingExecutor(dispatchQueue)

  val asyncCapacityRemaining = new AtomicLong(0L)

  def createUow() = new DelayableUOW(this)

  var uowEnqueueDelayReqested = 0L
  var uowEnqueueNodelayReqested = 0L
  var uowClosedCounter = 0L
  var uowCanceledCounter = 0L
  var uowStoringCounter = 0L
  var uowStoredCounter = 0L

  val uow_complete_latency = TimeMetric()

//  val closeSource = createSource(new ListEventAggregator[DelayableUOW](), dispatchQueue)
//  closeSource.setEventHandler(^{
//    closeSource.getData.foreach { uow =>
//      processClosed(uow)
//    }
//  });
//  closeSource.resume

  var pendingStores = new ConcurrentHashMap[MessageId, HashSet[DelayableUOW#MessageAction]]()

  var cancelable_enqueue_actions = new HashMap[QueueEntryKey, DelayableUOW#MessageAction]()

  val lastUowId = new AtomicInteger(1)

  var producerSequenceIdTracker = new ActiveMQMessageAuditNoSync

  def getLastProducerSequenceId(id: ProducerId): Long = dispatchQueue.sync {
    producerSequenceIdTracker.getLastSeqId(id)
  }

  def processClosed(uow:DelayableUOW) = {
    dispatchQueue.assertExecuting()
    uowClosedCounter += 1

    // Broker could issue a flush_message call before
    // this stage runs.. which make the stage jump over UowDelayed
    if( uow.state.stage < UowDelayed.stage ) {
      uow.state = UowDelayed
    }
    if( uow.state.stage < UowFlushing.stage ) {
      uow.actions.foreach { case (id, action) =>

        // The UoW may have been canceled.
        if( action.messageRecord!=null && action.enqueues.isEmpty ) {
          action.removeFromPendingStore()
          action.messageRecord = null
          uow.delayableActions -= 1
        }
        if( action.isEmpty ) {
          action.cancel()
        }

        // dequeues can cancel out previous enqueues
        action.dequeues.foreach { entry=>
          val entry_key = key(entry)
          val prev_action:DelayableUOW#MessageAction = cancelable_enqueue_actions.remove(entry_key)

          if( prev_action!=null ) {
            val prev_uow = prev_action.uow
            prev_uow.synchronized {
              if( !prev_uow.canceled ) {

                prev_uow.delayableActions -= 1

                // yay we can cancel out a previous enqueue
                prev_action.enqueues = prev_action.enqueues.filterNot( x=> key(x) == entry_key )
                if( prev_uow.state.stage >= UowDelayed.stage ) {

                  // if the message is not in any queues.. we can gc it..
                  if( prev_action.enqueues == Nil && prev_action.messageRecord !=null ) {
                    prev_action.removeFromPendingStore()
                    prev_action.messageRecord = null
                    prev_uow.delayableActions -= 1
                  }

                  // Cancel the action if it's now empty
                  if( prev_action.isEmpty ) {
                    prev_action.cancel()
                  } else if( !prev_uow.delayable ) {
                    // flush it if there is no point in delaying anymore
                    prev_uow.complete_asap
                  }
                }
              }
            }
            // since we canceled out the previous enqueue.. now cancel out the action
            action.dequeues = action.dequeues.filterNot( _ == entry)
            if( action.isEmpty ) {
              action.cancel()
            }
          }
        }
      }
    }

    if( !uow.canceled && uow.state.stage < UowFlushQueued.stage ) {
      if( uow.delayable ) {
        // Let the uow get GCed if its' canceled during the delay window..
        val ref = new WeakReference[DelayableUOW](uow)
        scheduleFlush(ref)
      } else {
        enqueueFlush(uow)
      }
    }
  }

  private def scheduleFlush(ref: WeakReference[DelayableUOW]) {
    dispatchQueue.executeAfter(flushDelay, TimeUnit.MILLISECONDS, ^ {
      val uow = ref.get();
      if (uow != null) {
        enqueueFlush(uow)
      }
    })
  }

  val flush_queue = new java.util.LinkedHashMap[Long,  DelayableUOW]()

  def enqueueFlush(uow:DelayableUOW) = {
    dispatchQueue.assertExecuting()
    if( uow!=null && !uow.canceled && uow.state.stage < UowFlushQueued.stage ) {
      uow.state = UowFlushQueued
      flush_queue.put (uow.uowId, uow)
      flushSource.merge(1)
    }
  }

  val flushSource = createSource(EventAggregators.INTEGER_ADD, dispatchQueue)
  flushSource.setEventHandler(^{drainFlushes});
  flushSource.resume

  def drainFlushes:Unit = {
    dispatchQueue.assertExecuting()

    // Some UOWs may have been canceled.
    import collection.JavaConversions._
    val values = flush_queue.values().toSeq.toArray
    flush_queue.clear()

    val uows = values.flatMap { uow=>
      if( uow.canceled ) {
        None
      } else {
        // It will not be possible to cancel the UOW anymore..
        uow.state = UowFlushing
        uow.actions.foreach { case (_, action) =>
          action.enqueues.foreach { queue_entry=>
            val action = cancelable_enqueue_actions.remove(key(queue_entry))
            assert(action!=null)
          }
        }
        if( !started ) {
          uow.onCompleted(new SuppressReplyException("Store stopped"))
          None
        } else {
          Some(uow)
        }
      }
    }

    if( !uows.isEmpty ) {
      uowStoringCounter += uows.size
      flushSource.suspend
      writeExecutor {
        val e = try {
          client.store(uows)
          null
        } catch {
          case e:Throwable => e
        }
        flushSource.resume
        dispatchQueue {
          uowStoredCounter += uows.size
          uows.foreach { uow=>
            uow.onCompleted(e)
          }
        }
      }
    }
  }

  var started = false
  def snappyCompressLogs = parent.snappyCompressLogs

  def start = {
    asyncCapacityRemaining.set(parent.asyncBufferSize)
    client.start()
    dispatchQueue.sync {
      started = true
      pollGc
      if(parent.monitorStats) {
        monitorStats
      }
    }
  }

  def stop() = {
    dispatchQueue.sync {
      started = false
    }
    client.stop()
  }

  def pollGc:Unit = dispatchQueue.after(10, TimeUnit.SECONDS) {
    if( started ) {
      val positions = parent.getTopicGCPositions
      writeExecutor {
        if( started ) {
          client.gc(positions)
          pollGc
        }
      }
    }
  }

  def monitorStats:Unit = dispatchQueue.after(1, TimeUnit.SECONDS) {
    if( started ) {
      println(("committed: %d, canceled: %d, storing: %d, stored: %d, " +
        "uow complete: %,.3f ms, " +
        "index write: %,.3f ms, " +
        "log write: %,.3f ms, log flush: %,.3f ms, log rotate: %,.3f ms"+
        "add msg: %,.3f ms, add enqueue: %,.3f ms, " +
        "uowEnqueueDelayReqested: %d, uowEnqueueNodelayReqested: %d "
        ).format(
          uowClosedCounter, uowCanceledCounter, uowStoringCounter, uowStoredCounter,
          uow_complete_latency.reset,
        client.max_index_write_latency.reset,
          client.log.max_log_write_latency.reset, client.log.max_log_flush_latency.reset, client.log.max_log_rotate_latency.reset,
        client.max_write_message_latency.reset, client.max_write_enqueue_latency.reset,
        uowEnqueueDelayReqested, uowEnqueueNodelayReqested
      ))
      uowClosedCounter = 0
//      uowCanceledCounter = 0
      uowStoringCounter = 0
      uowStoredCounter = 0
      monitorStats
    }
  }

  /////////////////////////////////////////////////////////////////////
  //
  // Implementation of the Store interface
  //
  /////////////////////////////////////////////////////////////////////

  def checkpoint(sync:Boolean) = writeExecutor.sync {
    client.snapshotIndex(sync)
  }

  def purge = writeExecutor.sync {
    client.purge
    lastCollectionKey.set(1)
  }

  def getLastQueueEntrySeq(key:Long) = {
    client.getLastQueueEntrySeq(key)
  }

  def collectionEmpty(key:Long) = writeExecutor.sync {
    client.collectionEmpty(key)
  }

  def collectionSize(key:Long) = {
    client.collectionSize(key)
  }

  def collectionIsEmpty(key:Long) = {
    client.collectionIsEmpty(key)
  }

  def cursorMessages(preparedAcks:java.util.HashSet[MessageId], key:Long, listener:MessageRecoveryListener, startPos:Long, endPos:Long=Long.MaxValue, max:Long=Long.MaxValue) = {
    var lastmsgid:MessageId = null
    var count = 0L
    client.queueCursor(key, startPos, endPos) { msg =>
      if( !preparedAcks.contains(msg.getMessageId) && listener.recoverMessage(msg) ) {
        lastmsgid = msg.getMessageId
        count += 1
      }
      count < max && listener.canRecoveryNextMessage
    }
    if( lastmsgid==null ) {
      startPos
    } else {
      lastmsgid.getEntryLocator.asInstanceOf[EntryLocator].seq+1
    }
  }

  def getXAActions(key:Long) = {
    val msgs = ListBuffer[Message]()
    val acks = ListBuffer[XaAckRecord]()
    client.transactionCursor(key) { command =>
      command match {
        case message:Message => msgs += message
        case record:XaAckRecord => acks += record
      }
      true
    }
    (msgs, acks)
  }

  def queuePosition(id: MessageId):Long = {
    id.getEntryLocator.asInstanceOf[EntryLocator].seq
  }

  def createQueueStore(dest:ActiveMQQueue):LevelDBStore#LevelDBMessageStore = {
    parent.createQueueMessageStore(dest, createCollection(utf8(dest.getQualifiedName), QUEUE_COLLECTION_TYPE))
  }
  def destroyQueueStore(key:Long) = writeExecutor.sync {
    client.removeCollection(key)
  }

  def getLogAppendPosition = writeExecutor.sync {
    client.getLogAppendPosition
  }

  def addSubscription(topic_key:Long, info:SubscriptionInfo):DurableSubscription = {
    val record = new SubscriptionRecord.Bean
    record.setTopicKey(topic_key)
    record.setClientId(info.getClientId)
    record.setSubscriptionName(info.getSubscriptionName)
    if( info.getSelector!=null ) {
      record.setSelector(info.getSelector)
    }
    if( info.getDestination!=null ) {
      record.setDestinationName(info.getDestination.getQualifiedName)
    }
    if ( info.getSubscribedDestination!=null) {
      record.setSubscribedDestinationName(info.getSubscribedDestination.getQualifiedName)
    }
    val collection = new CollectionRecord.Bean()
    collection.setType(SUBSCRIPTION_COLLECTION_TYPE)
    collection.setKey(lastCollectionKey.incrementAndGet())
    collection.setMeta(record.freeze().toUnframedBuffer)

    val buffer = collection.freeze()
    buffer.toFramedBuffer // eager encode the record.
    writeExecutor.sync {
      client.addCollection(buffer)
    }
    DurableSubscription(collection.getKey, topic_key, info)
  }

  def removeSubscription(sub:DurableSubscription) {
    writeExecutor.sync {
      client.removeCollection(sub.subKey)
    }
  }

  def createTopicStore(dest:ActiveMQTopic) = {
    var key = createCollection(utf8(dest.getQualifiedName), TOPIC_COLLECTION_TYPE)
    parent.createTopicMessageStore(dest, key)
  }

  def createCollection(name:Buffer, collectionType:Int) = {
    val collection = new CollectionRecord.Bean()
    collection.setType(collectionType)
    collection.setMeta(name)
    collection.setKey(lastCollectionKey.incrementAndGet())
    val buffer = collection.freeze()
    buffer.toFramedBuffer // eager encode the record.
    writeExecutor.sync {
      client.addCollection(buffer)
    }
    collection.getKey
  }

  def buffer(packet:ByteSequence) = new Buffer(packet.data, packet.offset, packet.length)

  def createTransactionContainer(id:XATransactionId) =
    createCollection(buffer(parent.wireFormat.marshal(id)), TRANSACTION_COLLECTION_TYPE)

  def removeTransactionContainer(key:Long) = writeExecutor.sync {
    client.removeCollection(key)
  }


  def loadCollections = {
    val collections = writeExecutor.sync {
      client.listCollections
    }
    var last = 0L
    collections.foreach { case (key, record) =>
      last = key
      record.getType match {
        case QUEUE_COLLECTION_TYPE =>
          val dest = ActiveMQDestination.createDestination(record.getMeta.utf8().toString, ActiveMQDestination.QUEUE_TYPE).asInstanceOf[ActiveMQQueue]
          parent.createQueueMessageStore(dest, key)
        case TOPIC_COLLECTION_TYPE =>
          val dest = ActiveMQDestination.createDestination(record.getMeta.utf8().toString, ActiveMQDestination.TOPIC_TYPE).asInstanceOf[ActiveMQTopic]
          parent.createTopicMessageStore(dest, key)
        case SUBSCRIPTION_COLLECTION_TYPE =>
          val sr = SubscriptionRecord.FACTORY.parseUnframed(record.getMeta)
          val info = new SubscriptionInfo
          info.setClientId(sr.getClientId)
          info.setSubscriptionName(sr.getSubscriptionName)
          if( sr.hasSelector ) {
            info.setSelector(sr.getSelector)
          }
          if( sr.hasDestinationName ) {
            info.setDestination(ActiveMQDestination.createDestination(sr.getDestinationName, ActiveMQDestination.TOPIC_TYPE))
          }
          if( sr.hasSubscribedDestinationName ) {
            info.setSubscribedDestination(ActiveMQDestination.createDestination(sr.getSubscribedDestinationName, ActiveMQDestination.TOPIC_TYPE))
          }

          var sub = DurableSubscription(key, sr.getTopicKey, info)
          sub.lastAckPosition = client.getAckPosition(key);
          sub.gcPosition = sub.lastAckPosition
          parent.createSubscription(sub)
        case TRANSACTION_COLLECTION_TYPE =>
          val meta = record.getMeta
          val txid = parent.wireFormat.unmarshal(new ByteSequence(meta.data, meta.offset, meta.length)).asInstanceOf[XATransactionId]
          val transaction = parent.transaction(txid)
          transaction.xacontainer_id = key
        case _ =>
      }
    }
    lastCollectionKey.set(last)
  }

  def createPList(name:String):LevelDBStore#LevelDBPList = {
    parent.createPList(name, lastPListKey.incrementAndGet())
  }

  def destroyPList(key:Long) = writeExecutor.sync {
    client.removePlist(key)
  }

  def plistPut(key:Array[Byte], value:Array[Byte]) = client.plistPut(key, value)
  def plistGet(key:Array[Byte]) = client.plistGet(key)
  def plistDelete(key:Array[Byte]) = client.plistDelete(key)
  def plistIterator = client.plistIterator

  def getMessage(x: MessageId):Message = {
    val id = Option(pendingStores.get(x)).flatMap(_.headOption).map(_.id).getOrElse(x)
    val locator = id.getDataLocator()
    val msg = client.getMessage(locator)
    if( msg!=null ) {
      msg.setMessageId(id)
    } else {
      LevelDBStore.warn("Could not load messages for: "+x+" at: "+locator)
    }
    msg
  }

}
