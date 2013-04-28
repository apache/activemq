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

import junit.framework.TestCase
import org.apache.activemq.broker._
import org.apache.activemq.store._
import java.io.File
import junit.framework.Assert._
import org.apache.commons.math.stat.descriptive.DescriptiveStatistics
import region.policy.{PolicyEntry, PolicyMap}
import org.apache.activemq.leveldb.{LevelDBStore}

/**
 * <p>
 * </p>
 *
 * @author <a href="http://hiramchirino.com">Hiram Chirino</a>
 */
class EnqueueRateScenariosTest extends TestCase {

  var broker: BrokerService = null

  override def setUp() {
    import collection.JavaConversions._
    broker = new BrokerService
    broker.setDeleteAllMessagesOnStartup(true)
    broker.setPersistenceAdapter(createStore)
    broker.addConnector("tcp://0.0.0.0:0")
//    val policies = new PolicyMap();
//    val entry = new PolicyEntry
//    entry.setQueue(">")
//    policies.setPolicyEntries(List(entry))
//    broker.setDestinationPolicy(policies)
    broker.start
    broker.waitUntilStarted()
  }

  override def tearDown() = {
    if (broker != null) {
      broker.stop
      broker.waitUntilStopped
    }
  }

  protected def canceledEnqueues() =
    broker.getPersistenceAdapter.asInstanceOf[LevelDBStore].db.uowCanceledCounter

  protected def enqueueOptimized() =
    broker.getPersistenceAdapter.asInstanceOf[LevelDBStore].db.uowEnqueueDelayReqested

  protected def enqueueNotOptimized() =
    broker.getPersistenceAdapter.asInstanceOf[LevelDBStore].db.uowEnqueueNodelayReqested


  protected def createStore: PersistenceAdapter = {
    var store: LevelDBStore = new LevelDBStore
    store.setDirectory(new File("target/activemq-data/leveldb"))
    return store
  }

  def collect_benchmark(scenario:ActiveMQScenario, warmup:Int, samples_count:Int) = {
    val (cancels, optimized, unoptimized) = scenario.with_load {
      println("Warming up for %d seconds...".format(warmup))
      Thread.sleep(warmup*1000)
      println("Sampling...")
      scenario.collection_start
      val cancelStart = canceledEnqueues
      val enqueueOptimizedStart = enqueueOptimized
      val enqueueNotOptimizedStart = enqueueNotOptimized
      for (i <- 0 until samples_count) {
        Thread.sleep(1000);
        scenario.collection_sample
      }
      (canceledEnqueues-cancelStart, enqueueOptimized-enqueueOptimizedStart, enqueueNotOptimized-enqueueNotOptimizedStart)
    }
    println("Done.")

    var samples = scenario.collection_end
    val error_rates = samples.get("e_custom").get.map(_._2)
    assertFalse("Errors occured during scenario run: "+error_rates, error_rates.find(_ > 0 ).isDefined )

    val producer_stats = new DescriptiveStatistics();
    for( producer_rates <- samples.get("p_custom") ) {
      for( i <- producer_rates ) {
        producer_stats.addValue(i._2)
      }
    }

    val consumer_stats = new DescriptiveStatistics();
    for( consumer_rates <- samples.get("c_custom") ) {
      for( i <- consumer_rates ) {
        consumer_stats.addValue(i._2)
      }
    }

    (producer_stats, consumer_stats, cancels*1.0/samples_count, optimized*1.0/samples_count, unoptimized*1.0/samples_count)
  }

  def benchmark(name:String, warmup:Int=3, samples_count:Int=15, async_send:Boolean=true)(setup:(ActiveMQScenario)=>Unit) = {
    println("Benchmarking: "+name)
    var options: String = "?jms.watchTopicAdvisories=false&jms.useAsyncSend="+async_send
    val url = broker.getTransportConnectors.get(0).getConnectUri + options

    val scenario = new ActiveMQScenario
    scenario.url = url
    scenario.display_errors = true
    scenario.persistent = true
    scenario.message_size = 1024 * 3

    setup(scenario)
    val (producer_stats, consumer_stats, cancels, optimized, unoptimized) = collect_benchmark(scenario, warmup, samples_count)

    println("%s: producer avg msg/sec: %,.2f, stddev: %,.2f".format(name, producer_stats.getMean, producer_stats.getStandardDeviation))
    println("%s: consumer avg msg/sec: %,.2f, stddev: %,.2f".format(name, consumer_stats.getMean, consumer_stats.getStandardDeviation))
    println("%s: canceled enqueues/sec: %,.2f".format(name,cancels))
    println("%s: optimized enqueues/sec: %,.2f".format(name,optimized))
    println("%s: unoptimized enqueues/sec: %,.2f".format(name,unoptimized))

    (producer_stats, consumer_stats, cancels, optimized, unoptimized)
  }

  def testHighCancelRatio = {
    val (producer_stats, consumer_stats, cancels, optimized, unoptimized) = benchmark("both_connected_baseline") { scenario=>
      scenario.producers = 1
      scenario.consumers = 1
    }
    val cancel_ratio = cancels / producer_stats.getMean
    assertTrue("Expecting more than 80%% of the enqueues get canceled. But only %.2f%% was canceled".format(cancel_ratio*100), cancel_ratio > .80)
  }

  def testDecoupledProducerRate = {

    // Fill up the queue with messages.. for the benefit of the next benchmark..
    val from_1_to_0 = benchmark("from_1_to_0", 60) { scenario=>
      scenario.producers = 1
      scenario.consumers = 0
    }
    val from_1_to_10 = benchmark("from_1_to_10") { scenario=>
      scenario.producers = 1
      scenario.consumers = 10
    }
    val from_1_to_1 = benchmark("from_1_to_1") { scenario=>
      scenario.producers = 1
      scenario.consumers = 1
    }

    var percent_diff0 = (1.0 - (from_1_to_0._1.getMean / from_1_to_1._1.getMean)).abs * 100
    var percent_diff1 = (1.0 - (from_1_to_1._1.getMean / from_1_to_10._1.getMean)).abs * 100

    var msg0 = "The 0 vs 1 consumer scenario producer rate was within %.2f%%".format(percent_diff0)
    var msg1 = "The 1 vs 10 consumer scenario producer rate was within %.2f%%".format(percent_diff1)

    println(msg0)
    println(msg1)

    assertTrue(msg0, percent_diff0 <= 60)
    assertTrue(msg1, percent_diff1 <= 20)
  }

}