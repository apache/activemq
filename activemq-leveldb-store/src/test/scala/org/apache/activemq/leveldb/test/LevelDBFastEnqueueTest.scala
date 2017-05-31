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
package org.apache.activemq.leveldb.test

import org.apache.activemq.ActiveMQConnection
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.activemq.broker.BrokerService
import org.apache.activemq.command.ActiveMQQueue
import org.apache.activemq.command.ConnectionControl
import org.junit.After
import org.junit.Before
import org.junit.Test
import javax.jms._
import java.io.File
import java.util.Vector
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong
import junit.framework.Assert._
import org.apache.activemq.leveldb.util.Log
import junit.framework.TestCase
import org.apache.activemq.leveldb.LevelDBStore

object LevelDBFastEnqueueTest extends Log
class LevelDBFastEnqueueTest extends TestCase {

  import LevelDBFastEnqueueTest._

  @Test def testPublishNoConsumer: Unit = {
    startBroker(true, 10)
    val sharedCount: AtomicLong = new AtomicLong(toSend)
    var start: Long = System.currentTimeMillis
    var executorService: ExecutorService = Executors.newCachedThreadPool
    var i: Int = 0
    while (i < parallelProducer) {
      executorService.execute(new Runnable {
        def run: Unit = {
          try {
            publishMessages(sharedCount, 0)
          }
          catch {
            case e: Exception => {
              exceptions.add(e)
            }
          }
        }
      })
      i += 1
    }
    executorService.shutdown
    executorService.awaitTermination(30, TimeUnit.MINUTES)
    assertTrue("Producers done in time", executorService.isTerminated)
    assertTrue("No exceptions: " + exceptions, exceptions.isEmpty)
    var totalSent: Long = toSend * payloadString.length
    var duration: Double = System.currentTimeMillis - start
    info("Duration:                " + duration + "ms")
    info("Rate:                       " + (toSend * 1000 / duration) + "m/s")
    info("Total send:             " + totalSent)
    info("Total journal write: " + store.getLogAppendPosition)
    info("Journal writes %:    " + store.getLogAppendPosition / totalSent.asInstanceOf[Double] * 100 + "%")
    stopBroker
    restartBroker(0, 1200000)
    consumeMessages(toSend)
  }

  @Test def testPublishNoConsumerNoCheckpoint: Unit = {
    toSend = 100
    startBroker(true, 0)
    val sharedCount: AtomicLong = new AtomicLong(toSend)
    var start: Long = System.currentTimeMillis
    var executorService: ExecutorService = Executors.newCachedThreadPool
    var i: Int = 0
    while (i < parallelProducer) {
      executorService.execute(new Runnable {
        def run: Unit = {
          try {
            publishMessages(sharedCount, 0)
          }
          catch {
            case e: Exception => {
              exceptions.add(e)
            }
          }
        }
      })
      i += 1;
    }
    executorService.shutdown
    executorService.awaitTermination(30, TimeUnit.MINUTES)
    assertTrue("Producers done in time", executorService.isTerminated)
    assertTrue("No exceptions: " + exceptions, exceptions.isEmpty)
    var totalSent: Long = toSend * payloadString.length
    broker.getAdminView.gc
    var duration: Double = System.currentTimeMillis - start
    info("Duration:                " + duration + "ms")
    info("Rate:                       " + (toSend * 1000 / duration) + "m/s")
    info("Total send:             " + totalSent)
    info("Total journal write: " + store.getLogAppendPosition)
    info("Journal writes %:    " + store.getLogAppendPosition / totalSent.asInstanceOf[Double] * 100 + "%")
    stopBroker
    restartBroker(0, 0)
    consumeMessages(toSend)
  }

  private def consumeMessages(count: Long): Unit = {
    var connection: ActiveMQConnection = connectionFactory.createConnection.asInstanceOf[ActiveMQConnection]
    connection.setWatchTopicAdvisories(false)
    connection.start
    var session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    var consumer: MessageConsumer = session.createConsumer(destination)
    var i: Int = 0
    while (i < count) {
        assertNotNull("got message " + i, consumer.receive(10000))
        i += 1;
    }
    assertNull("none left over", consumer.receive(2000))
  }

  protected def restartBroker(restartDelay: Int, checkpoint: Int): Unit = {
    stopBroker
    TimeUnit.MILLISECONDS.sleep(restartDelay)
    startBroker(false, checkpoint)
  }

  override def tearDown() = stopBroker

  def stopBroker: Unit = {
    if (broker != null) {
      broker.stop
      broker.waitUntilStopped
    }
  }

  private def publishMessages(count: AtomicLong, expiry: Int): Unit = {
    var connection: ActiveMQConnection = connectionFactory.createConnection.asInstanceOf[ActiveMQConnection]
    connection.setWatchTopicAdvisories(false)
    connection.start
    var session: Session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    var producer: MessageProducer = session.createProducer(destination)
    var start: Long = System.currentTimeMillis
    var i: Long = 0l
    var bytes: Array[Byte] = payloadString.getBytes
    while ((({
      i = count.getAndDecrement; i
    })) > 0) {
      var message: Message = null
      if (useBytesMessage) {
        message = session.createBytesMessage
        (message.asInstanceOf[BytesMessage]).writeBytes(bytes)
      }
      else {
        message = session.createTextMessage(payloadString)
      }
      producer.send(message, DeliveryMode.PERSISTENT, 5, expiry)
      if (i != toSend && i % sampleRate == 0) {
        var now: Long = System.currentTimeMillis
        info("Remainder: " + i + ", rate: " + sampleRate * 1000 / (now - start) + "m/s")
        start = now
      }
    }
    connection.syncSendPacket(new ConnectionControl)
    connection.close
  }

  def startBroker(deleteAllMessages: Boolean, checkPointPeriod: Int): Unit = {
    broker = new BrokerService
    broker.setDeleteAllMessagesOnStartup(deleteAllMessages)
    store = createStore
    broker.setPersistenceAdapter(store)
    broker.addConnector("tcp://0.0.0.0:0")
    broker.start
    var options: String = "?jms.watchTopicAdvisories=false&jms.useAsyncSend=true&jms.alwaysSessionAsync=false&jms.dispatchAsync=false&socketBufferSize=131072&ioBufferSize=16384&wireFormat.tightEncodingEnabled=false&wireFormat.cacheSize=8192"
    connectionFactory = new ActiveMQConnectionFactory(broker.getTransportConnectors.get(0).getConnectUri + options)
  }



  protected def createStore: LevelDBStore = {
    var store: LevelDBStore = new LevelDBStore
    store.setDirectory(new File("target/activemq-data/leveldb"))
    return store
  }

  private[leveldb] var broker: BrokerService = null
  private[leveldb] var connectionFactory: ActiveMQConnectionFactory = null
  private[leveldb] var store: LevelDBStore = null
  private[leveldb] var destination: Destination = new ActiveMQQueue("Test")
  private[leveldb] var payloadString: String = new String(new Array[Byte](6 * 1024))
  private[leveldb] var useBytesMessage: Boolean = true
  private[leveldb] final val parallelProducer: Int = 20
  private[leveldb] var exceptions: Vector[Exception] = new Vector[Exception]
  private[leveldb] var toSend: Long = 100000
  private[leveldb] final val sampleRate: Double = 100000
}