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

import java.util.concurrent.atomic._
import java.util.concurrent.TimeUnit._
import scala.collection.mutable.ListBuffer

object Scenario {
  val MESSAGE_ID:Array[Byte] = "message-id"
  val NEWLINE = '\n'.toByte
  val NANOS_PER_SECOND = NANOSECONDS.convert(1, SECONDS)
  
  implicit def toBytes(value: String):Array[Byte] = value.getBytes("UTF-8")

  def o[T](value:T):Option[T] = value match {
    case null => None
    case x => Some(x)
  }
}

trait Scenario {
  import Scenario._

  var url:String = "tcp://localhost:61616"
  var user_name:String = _
  var password:String = _

  private var _producer_sleep: { def apply(): Int; def init(time: Long) } = new { def apply() = 0; def init(time: Long) {}  }
  def producer_sleep = _producer_sleep()
  def producer_sleep_= (new_value: Int) = _producer_sleep = new { def apply() = new_value; def init(time: Long) {}  }
  def producer_sleep_= (new_func: { def apply(): Int; def init(time: Long) }) = _producer_sleep = new_func

  private var _consumer_sleep: { def apply(): Int; def init(time: Long) } = new { def apply() = 0; def init(time: Long) {}  }
  def consumer_sleep = _consumer_sleep()
  def consumer_sleep_= (new_value: Int) = _consumer_sleep = new { def apply() = new_value; def init(time: Long) {}  }
  def consumer_sleep_= (new_func: { def apply(): Int; def init(time: Long) }) = _consumer_sleep = new_func

  var producers = 1
  var producers_per_sample = 0

  var consumers = 1
  var consumers_per_sample = 0
  var sample_interval = 1000

  var message_size = 1024
  var persistent = false

  var headers = Array[Array[(String,String)]]()
  var selector:String = null
  var no_local = false
  var durable = false
  var ack_mode = "auto"
  var messages_per_connection = -1L
  var display_errors = false

  var destination_type = "queue"
  private var _destination_name: () => String = () => "load"
  def destination_name = _destination_name()
  def destination_name_=(new_name: String) = _destination_name = () => new_name
  def destination_name_=(new_func: () => String) = _destination_name = new_func
  var destination_count = 1

  val producer_counter = new AtomicLong()
  val consumer_counter = new AtomicLong()
  val error_counter = new AtomicLong()
  val done = new AtomicBoolean()

  var queue_prefix = ""
  var topic_prefix = ""
  var name = "custom"

  var drain_timeout = 2000L

  def run() = {
    print(toString)
    println("--------------------------------------")
    println("     Running: Press ENTER to stop")
    println("--------------------------------------")
    println("")

    with_load {

      // start a sampling client...
      val sample_thread = new Thread() {
        override def run() = {
          
          def print_rate(name: String, periodCount:Long, totalCount:Long, nanos: Long) = {

            val rate_per_second: java.lang.Float = ((1.0f * periodCount / nanos) * NANOS_PER_SECOND)
            println("%s total: %,d, rate: %,.3f per second".format(name, totalCount, rate_per_second))
          }

          try {
            var start = System.nanoTime
            var total_producer_count = 0L
            var total_consumer_count = 0L
            var total_error_count = 0L
            collection_start
            while( !done.get ) {
              Thread.sleep(sample_interval)
              val end = System.nanoTime
              collection_sample
              val samples = collection_end
              samples.get("p_custom").foreach { case (_, count)::Nil =>
                total_producer_count += count
                print_rate("Producer", count, total_producer_count, end - start)
              case _ =>
              }
              samples.get("c_custom").foreach { case (_, count)::Nil =>
                total_consumer_count += count
                print_rate("Consumer", count, total_consumer_count, end - start)
              case _ =>
              }
              samples.get("e_custom").foreach { case (_, count)::Nil =>
                if( count!= 0 ) {
                  total_error_count += count
                  print_rate("Error", count, total_error_count, end - start)
                }
              case _ =>
              }
              start = end
            }
          } catch {
            case e:InterruptedException =>
          }
        }
      }
      sample_thread.start()

      System.in.read()
      done.set(true)

      sample_thread.interrupt
      sample_thread.join
    }

  }

  override def toString() = {
    "--------------------------------------\n"+
    "Scenario Settings\n"+
    "--------------------------------------\n"+
    "  destination_type      = "+destination_type+"\n"+
    "  queue_prefix          = "+queue_prefix+"\n"+
    "  topic_prefix          = "+topic_prefix+"\n"+
    "  destination_count     = "+destination_count+"\n" +
    "  destination_name      = "+destination_name+"\n" +
    "  sample_interval (ms)  = "+sample_interval+"\n" +
    "  \n"+
    "  --- Producer Properties ---\n"+
    "  producers             = "+producers+"\n"+
    "  message_size          = "+message_size+"\n"+
    "  persistent            = "+persistent+"\n"+
    "  producer_sleep (ms)   = "+producer_sleep+"\n"+
    "  headers               = "+headers.mkString(", ")+"\n"+
    "  \n"+
    "  --- Consumer Properties ---\n"+
    "  consumers             = "+consumers+"\n"+
    "  consumer_sleep (ms)   = "+consumer_sleep+"\n"+
    "  selector              = "+selector+"\n"+
    "  durable               = "+durable+"\n"+
    ""

  }

  protected def headers_for(i:Int) = {
    if ( headers.isEmpty ) {
      Array[(String, String)]()
    } else {
      headers(i%headers.size)
    }
  }

  var producer_samples:Option[ListBuffer[(Long,Long)]] = None
  var consumer_samples:Option[ListBuffer[(Long,Long)]] = None
  var error_samples = ListBuffer[(Long,Long)]()

  def collection_start: Unit = {
    producer_counter.set(0)
    consumer_counter.set(0)
    error_counter.set(0)

    producer_samples = if (producers > 0 || producers_per_sample>0 ) {
      Some(ListBuffer[(Long,Long)]())
    } else {
      None
    }
    consumer_samples = if (consumers > 0 || consumers_per_sample>0 ) {
      Some(ListBuffer[(Long,Long)]())
    } else {
      None
    }
  }

  def collection_end: Map[String, scala.List[(Long,Long)]] = {
    var rc = Map[String, List[(Long,Long)]]()
    producer_samples.foreach{ samples =>
      rc += "p_"+name -> samples.toList
      samples.clear
    }
    consumer_samples.foreach{ samples =>
      rc += "c_"+name -> samples.toList
      samples.clear
    }
    rc += "e_"+name -> error_samples.toList
    error_samples.clear
    rc
  }

  trait Client {
    def start():Unit
    def shutdown():Unit
  }

  var producer_clients = List[Client]()
  var consumer_clients = List[Client]()

  def with_load[T](func: =>T ):T = {
    done.set(false)

    _producer_sleep.init(System.currentTimeMillis())
    _consumer_sleep.init(System.currentTimeMillis())

    for (i <- 0 until producers) {
      val client = createProducer(i)
      producer_clients ::= client
      client.start()
    }

    for (i <- 0 until consumers) {
      val client = createConsumer(i)
      consumer_clients ::= client
      client.start()
    }

    try {
      func
    } finally {
      done.set(true)
      // wait for the threads to finish..
      for( client <- consumer_clients ) {
        client.shutdown
      }
      consumer_clients = List()
      for( client <- producer_clients ) {
        client.shutdown
      }
      producer_clients = List()
    }
  }

  def drain = {
    done.set(false)
    if( destination_type=="queue" || destination_type=="raw_queue" || durable==true ) {
      print("draining")
      consumer_counter.set(0)
      var consumer_clients = List[Client]()
      for (i <- 0 until destination_count) {
        val client = createConsumer(i)
        consumer_clients ::= client
        client.start()
      }

      // Keep sleeping until we stop draining messages.
      var drained = 0L
      try {
        Thread.sleep(drain_timeout);
        def done() = {
          val c = consumer_counter.getAndSet(0)
          drained += c
          c == 0
        }
        while( !done ) {
          print(".")
          Thread.sleep(drain_timeout);
        }
      } finally {
        done.set(true)
        for( client <- consumer_clients ) {
          client.shutdown
        }
        println(". (drained %d)".format(drained))
      }
    }
  }


  def collection_sample: Unit = {

    val now = System.currentTimeMillis()
    producer_samples.foreach(_.append((now, producer_counter.getAndSet(0))))
    consumer_samples.foreach(_.append((now, consumer_counter.getAndSet(0))))
    error_samples.append((now, error_counter.getAndSet(0)))

    // we might need to increment number the producers..
    for (i <- 0 until producers_per_sample) {
      val client = createProducer(producer_clients.length)
      producer_clients ::= client
      client.start()
    }

    // we might need to increment number the consumers..
    for (i <- 0 until consumers_per_sample) {
      val client = createConsumer(consumer_clients.length)
      consumer_clients ::= client
      client.start()
    }

  }
  
  def createProducer(i:Int):Client
  def createConsumer(i:Int):Client

}


