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
package org.apache.activemq.broker.region;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.BytesMessage;
import jakarta.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQTopic;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;

/**
 * RAW topic-send throughput. Everything that adds latency is removed:
 * non-persistent broker + NON_PERSISTENT messages (no store), async send with an
 * unbounded producer window (no per-send round trip), vm:// transport (no network),
 * producer flow control off, advisories/JMX/scheduler/stats/audit off, a single
 * reused small message with ids/timestamps disabled. What remains is the cost of
 * {@code Topic.doMessageSend()} itself.
 *
 * <p>{@code numConsumers=0} = pure send ceiling (empty dispatch); {@code >=1} =
 * raw end-to-end delivery to fast, high-prefetch, no-op consumers.
 *
 * <p>Named {@code *Benchmark.java} so surefire's {@code **}/{@code *Test.*} skips it in CI.
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@State(Scope.Benchmark)
public class TopicRawThroughputBenchmark {

    static final String TOPIC = "RAW.BENCH";

    @Param({"false", "true"})
    boolean useVirtualThread;

    /** false = one shared topic (single send lock); true = a distinct topic per producer thread. */
    @Param({"false", "true"})
    boolean distinctTopics;

    private final java.util.concurrent.atomic.AtomicInteger threadCounter =
            new java.util.concurrent.atomic.AtomicInteger();

    /** vm transport async dispatch: true = hand off to task-runner pool (SynchronousQueue);
     *  false = process the send on the calling producer thread (no handoff). */
    @Param({"true", "false"})
    boolean vmAsync;

    /** 0 = send ceiling (no dispatch); >=1 = end-to-end to that many fast consumers. */
    @Param({"0", "1"})
    int numConsumers;

    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    private final List<Connection> consumerConnections = new ArrayList<>();

    @State(Scope.Thread)
    public static class ThreadState {
        Connection connection;
        Session session;
        MessageProducer producer;
        BytesMessage message;   // reused every send

        @Setup(Level.Trial)
        public void setup(final TopicRawThroughputBenchmark b) throws Exception {
            connection = b.connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final int id = b.threadCounter.getAndIncrement();
            final String topicName = b.distinctTopics ? TOPIC + "." + id : TOPIC;
            final Topic topic = session.createTopic(topicName);
            producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
            producer.setDisableMessageID(true);
            producer.setDisableMessageTimestamp(true);
            // small raw byte body; created once and reused (marshalled once)
            final byte[] payload = new byte[128];
            java.util.Arrays.fill(payload, (byte) 'x');
            final BytesMessage bm = session.createBytesMessage();
            bm.writeBytes(payload);
            message = bm;
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (producer != null) producer.close();
            if (session != null) session.close();
            if (connection != null) connection.close();
        }
    }

    @Setup(Level.Trial)
    public void setupBroker() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("rawbench");
        broker.setPersistent(false);          // no journal / no store
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
        broker.setSchedulerSupport(false);
        broker.setEnableStatistics(false);
        broker.setUseShutdownHook(false);
        broker.setDeleteAllMessagesOnStartup(true);

        if (useVirtualThread) {
            broker.setVirtualThreadTaskRunner(true);
        } else {
            broker.setDedicatedTaskRunner(false);
        }

        final PolicyEntry topicPolicy = new PolicyEntry();
        topicPolicy.setTopic(">");
        topicPolicy.setProducerFlowControl(false);   // never throttle producers
        topicPolicy.setEnableAudit(false);
        topicPolicy.setExpireMessagesPeriod(0L);
        topicPolicy.setOptimizedDispatch(true);
        topicPolicy.setMemoryLimit(256L * 1024 * 1024);
        final PolicyMap policyMap = new PolicyMap();
        policyMap.put(new ActiveMQTopic(">"), topicPolicy);
        broker.setDestinationPolicy(policyMap);

        broker.addConnector("vm://rawbench?async=" + vmAsync);
        broker.start();
        broker.waitUntilStarted();

        connectionFactory = new ActiveMQConnectionFactory("vm://rawbench?create=false&async=" + vmAsync);
        connectionFactory.setUseAsyncSend(true);       // pipeline sends, no per-send round trip
        connectionFactory.setProducerWindowSize(0);    // unbounded async window
        connectionFactory.setWatchTopicAdvisories(false);
        connectionFactory.setAlwaysSyncSend(false);
        connectionFactory.getPrefetchPolicy().setTopicPrefetch(50000);

        for (int i = 0; i < numConsumers; i++) {
            final Connection conn = connectionFactory.createConnection();
            conn.start();
            final Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final MessageConsumer consumer = sess.createConsumer(sess.createTopic(TOPIC));
            consumer.setMessageListener(m -> { });     // no-op, drains as fast as dispatched
            consumerConnections.add(conn);
        }
    }

    @TearDown(Level.Trial)
    public void tearDownBroker() throws Exception {
        for (final Connection c : consumerConnections) {
            try { c.close(); } catch (final Exception ignore) { }
        }
        consumerConnections.clear();
        if (broker != null) { broker.stop(); broker.waitUntilStopped(); }
    }

    private void send(final ThreadState s) throws Exception {
        s.producer.send(s.message);
    }

    @Benchmark @Threads(1)  public void send_01_thread(final ThreadState s) throws Exception { send(s); }
    @Benchmark @Threads(4)  public void send_04_threads(final ThreadState s) throws Exception { send(s); }
    @Benchmark @Threads(8)  public void send_08_threads(final ThreadState s) throws Exception { send(s); }
    @Benchmark @Threads(16) public void send_16_threads(final ThreadState s) throws Exception { send(s); }
    @Benchmark @Threads(2)  public void send_02_threads(final ThreadState s) throws Exception { send(s); }
    @Benchmark @Threads(11) public void send_11_threads(final ThreadState s) throws Exception { send(s); }
    @Benchmark @Threads(22) public void send_22_threads(final ThreadState s) throws Exception { send(s); }
}
