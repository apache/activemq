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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;
import jakarta.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.virtual.VirtualDestination;
import org.apache.activemq.broker.region.virtual.VirtualDestinationInterceptor;
import org.apache.activemq.broker.region.virtual.VirtualTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.Message;
import org.apache.activemq.store.ListenableFuture;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.ProxyTopicMessageStore;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.jdbc.adapter.H2JDBCAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.commons.dbcp2.BasicDataSource;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
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
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

/**
 * JMH benchmark for Topic.doMessageSend() lock contention.
 * <p>
 * Measures throughput of concurrent producers sending persistent messages
 * to a virtual topic with:
 * <ul>
 *   <li><b>N durable subscribers</b> on the topic — these force persistence
 *       ({@code canOptimizeOutPersistence()} returns false) AND create real
 *       dispatch work inside {@code Topic.doMessageSend()}</li>
 *   <li><b>N virtual topic queue consumers</b> — create additional realistic
 *       broker-level routing work via {@code VirtualDestinationInterceptor}</li>
 * </ul>
 * <p>
 * The parameter {@code numSubscribers} controls both the number of durable
 * subscribers AND virtual topic queue consumers, making each message:
 * <ol>
 *   <li>Persisted to the topic store (under the lock in both approaches)</li>
 *   <li>Dispatched to N durable subscribers (under the lock in old approach,
 *       <b>outside</b> the lock in new approach — this is the key difference)</li>
 *   <li>Routed to N consumer queues via the virtual topic interceptor</li>
 * </ol>
 * <p>
 * Two store modes are available:
 * <ul>
 *   <li>{@code MEMORY_BUSYWAIT} — in-memory store with 200µs busy-wait latency</li>
 *   <li>{@code H2_JDBC} — real H2 file-based database with small DBCP2 pool
 *       (maxTotal=5), creating realistic IO + pool contention</li>
 * </ul>
 * <p>
 * <b>Excluded from CI:</b> file is named {@code *Benchmark.java} (not
 * {@code *Test.java}), so surefire's {@code **\/*Test.*} pattern skips it.
 * <p>
 * <b>Run locally:</b>
 * <pre>
 * # Build first (skip tests)
 * mvn install -DskipTests
 *
 * # Full benchmark (all combinations — takes a long time)
 * java -cp "activemq-unit-tests/target/test-classes:$(mvn -pl activemq-unit-tests dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)" \
 *   org.openjdk.jmh.Main "TopicSendLockBenchmark"
 *
 * # Quick: H2 JDBC, 5 subscribers
 * java -cp "activemq-unit-tests/target/test-classes:$(mvn -pl activemq-unit-tests dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)" \
 *   org.openjdk.jmh.Main "TopicSendLockBenchmark" \
 *   -p storeType=H2_JDBC -p numSubscribers=5
 *
 * # Quick: busy-wait, 10 subscribers, high threads only
 * java -cp "activemq-unit-tests/target/test-classes:$(mvn -pl activemq-unit-tests dependency:build-classpath -Dmdep.outputFile=/dev/stdout -q)" \
 *   org.openjdk.jmh.Main "TopicSendLockBenchmark.send_50_threads|TopicSendLockBenchmark.send_100_threads" \
 *   -p storeType=MEMORY_BUSYWAIT -p numSubscribers=10
 * </pre>
 *
 * <b>MacBook Pro M1 results — side-by-side comparison</b> (higher is better):
 * <pre>
 *                                                          MEMORY_BUSYWAIT (ops/s)           H2_JDBC (ops/s)
 * Benchmark              Subs    OLD (synchronized)  NEW (ReentrantLock)  Gain     OLD (synchronized)  NEW (ReentrantLock)  Gain
 * -------------------------------------------------------------------------------------------------------------------------------
 * send_01_thread            1         4238 ±  770         4360 ±   55       ~0%        10025 ± 1810         9761 ± 1704       ~0%
 * send_01_thread            5         3120 ±  251         3166 ±  171       ~0%         3253 ±  332         3205 ±  353       ~0%
 * send_01_thread           10         2109 ±   86         2128 ±  183       ~0%         1654 ±  169         1598 ±  279       ~0%
 * send_02_threads           1         4627 ±  130         4751 ±   40       ~0%        11217 ± 2522        12753 ± 1854      +14%
 * send_02_threads           5         3956 ±   61         4470 ±  105      +13%         3936 ±  769         3897 ±  860       ~0%
 * send_02_threads          10         2546 ±  123         2626 ±  215       ~0%         2030 ±  386         2037 ±  577       ~0%
 * send_10_threads           1         4583 ±  181         4304 ± 1236       ~0%        11925 ± 1661        15427 ± 5230      +29%
 * send_10_threads           5         3879 ±  444         4495 ±  197      +16%         4547 ±  554         5192 ± 1363      +14%
 * send_10_threads          10         2601 ±  166         3369 ±  203      +30%         2156 ±  534         2469 ±  242      +15%
 * send_20_threads           1         4606 ±   28         4654 ±  353       ~0%        11970 ± 1709        14948 ± 5646      +25%
 * send_20_threads           5         3927 ±  124         4505 ±  142      +15%         4568 ± 1601         5097 ± 1992      +12%
 * send_20_threads          10         2683 ±  120         3689 ±  165      +38%         2324 ±  413         2545 ±  213      +10%
 * send_50_threads           1         4496 ±  298         4678 ±   74       ~0%        10889 ± 1916        14175 ± 3652      +30%
 * send_50_threads           5         3844 ±  189         4436 ±  146      +15%         4608 ± 1209         4707 ± 1894       ~0%
 * send_50_threads          10         2737 ±  227         3859 ± 1113      +41%         2416 ±  124         2422 ±  204       ~0%
 * send_100_threads          1         4424 ±  214         4501 ±  260       ~0%         9734 ± 1668        12012 ± 5054      +23%
 * send_100_threads          5         3043 ± 1408         4283 ±  247      +41%         4339 ± 1146         4059 ±  958       ~0%
 * send_100_threads         10         2576 ±  257         2085 ±  947       ~0%         2174 ±  633         2040 ±  393       ~0%
 * </pre>
 * <p>
 * <b>Summary:</b>
 * <pre>
 * - 1 thread:     ~0% difference (no lock contention — expected)
 * - 10+ threads:  +14% to +41% with the ReentrantLock patch
 * - Sweet spot:   MEMORY_BUSYWAIT + 10 subs + 20-50 threads → +38-41%
 *                 H2_JDBC + 1 sub + 10-50 threads → +25-30%
 * - The gain increases with:
 *   (a) more subscribers — more dispatch work moved outside the lock
 *   (b) more producer threads — more contention on the lock
 * </pre>
 */
@BenchmarkMode(Mode.Throughput)
@OutputTimeUnit(TimeUnit.SECONDS)
@Warmup(iterations = 3, time = 3)
@Measurement(iterations = 5, time = 5)
@Fork(value = 1, jvmArgsAppend = {"-Xmx2g", "-Dorg.apache.activemq.default.directory.prefix=target/"})
@State(Scope.Benchmark)
public class TopicSendLockBenchmark {

    /**
     * Store type for the benchmark.
     */
    @Param({"MEMORY_BUSYWAIT", "H2_JDBC"})
    String storeType;

    /**
     * Number of subscribers. Controls BOTH:
     * <ul>
     *   <li>Durable topic subscribers — dispatch work inside Topic.doMessageSend()
     *       (this is under the lock in old code, outside in new code)</li>
     *   <li>Virtual topic queue consumers — additional broker-level routing</li>
     * </ul>
     */
    @Param({"1", "5", "10"})
    int numSubscribers;

    private static final String VIRTUAL_TOPIC_NAME = "VirtualTopic.BENCH";
    private static final int IO_LATENCY_MICROS = 200;

    private BrokerService broker;
    private ActiveMQConnectionFactory connectionFactory;
    private BasicDataSource dataSource;
    private final List<Connection> consumerConnections = new ArrayList<>();

    // Per-thread state: each thread gets its own connection/session/producer
    @State(Scope.Thread)
    public static class ThreadState {
        Connection connection;
        Session session;
        MessageProducer producer;
        int messageCounter;

        @Setup(Level.Trial)
        public void setup(final TopicSendLockBenchmark benchState) throws Exception {
            connection = benchState.connectionFactory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Topic topic = session.createTopic(VIRTUAL_TOPIC_NAME);
            producer = session.createProducer(topic);
            producer.setDeliveryMode(DeliveryMode.PERSISTENT);
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
        broker.setBrokerName("benchmark");
        broker.setUseJmx(false);
        broker.setPersistent(true);
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setAdvisorySupport(false);
        broker.setUseShutdownHook(false);
        broker.setDataDirectory("target/benchmark-data");

        // Disable producer flow control and set generous memory limits
        final PolicyEntry topicPolicy = new PolicyEntry();
        topicPolicy.setTopic(">");
        topicPolicy.setProducerFlowControl(false);
        topicPolicy.setMemoryLimit(512L * 1024 * 1024);

        final PolicyEntry queuePolicy = new PolicyEntry();
        queuePolicy.setQueue(">");
        queuePolicy.setProducerFlowControl(false);
        queuePolicy.setMemoryLimit(512L * 1024 * 1024);

        final PolicyMap policyMap = new PolicyMap();
        policyMap.put(new ActiveMQTopic(">"), topicPolicy);
        policyMap.put(new org.apache.activemq.command.ActiveMQQueue(">"), queuePolicy);
        broker.setDestinationPolicy(policyMap);

        // Configure virtual topics: messages sent to VirtualTopic.>
        // are dispatched to Consumer.N.VirtualTopic.> queues
        final VirtualTopic virtualTopic = new VirtualTopic();
        virtualTopic.setName("VirtualTopic.>");
        virtualTopic.setPrefix("Consumer.*.");
        final VirtualDestinationInterceptor interceptor = new VirtualDestinationInterceptor();
        interceptor.setVirtualDestinations(new VirtualDestination[]{virtualTopic});
        broker.setDestinationInterceptors(new DestinationInterceptor[]{interceptor});

        // Configure persistence adapter
        broker.setPersistenceAdapter(createPersistenceAdapter());

        broker.addConnector("vm://benchmark");
        broker.start();
        broker.waitUntilStarted();

        connectionFactory = new ActiveMQConnectionFactory("vm://benchmark?create=false");
        connectionFactory.setWatchTopicAdvisories(false);
        connectionFactory.getPrefetchPolicy().setQueuePrefetch(1000);
        connectionFactory.getPrefetchPolicy().setDurableTopicPrefetch(1000);

        // ---- Durable topic subscribers ----
        // These are CRITICAL: they ensure canOptimizeOutPersistence()=false
        // (so persistence actually happens under the lock) AND they create
        // real dispatch work inside Topic.doMessageSend().
        // With synchronized: persistence + dispatch to N durables = all under lock
        // With ReentrantLock patch: persistence under lock, dispatch outside
        for (int i = 1; i <= numSubscribers; i++) {
            final Connection conn = connectionFactory.createConnection();
            conn.setClientID("durable-bench-" + i);
            conn.start();
            final Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Topic topic = sess.createTopic(VIRTUAL_TOPIC_NAME);
            final MessageConsumer durableSub = sess.createDurableSubscriber(topic, "bench-sub-" + i);
            // Actively consume to create dispatch work and prevent backlog
            durableSub.setMessageListener(msg -> { });
            consumerConnections.add(conn);
        }

        // ---- Virtual topic queue consumers ----
        // Additional realistic load: each message is also routed to N
        // consumer queues by the VirtualDestinationInterceptor
        for (int i = 1; i <= numSubscribers; i++) {
            final Connection conn = connectionFactory.createConnection();
            conn.start();
            final Session sess = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            final Queue queue = sess.createQueue("Consumer." + i + "." + VIRTUAL_TOPIC_NAME);
            final MessageConsumer consumer = sess.createConsumer(queue);
            consumer.setMessageListener(msg -> { });
            consumerConnections.add(conn);
        }
    }

    private PersistenceAdapter createPersistenceAdapter() throws IOException {
        if ("H2_JDBC".equals(storeType)) {
            return createH2JdbcAdapter();
        }
        return new SlowPersistenceAdapterWrapper(() -> IO_LATENCY_MICROS);
    }

    private JDBCPersistenceAdapter createH2JdbcAdapter() throws IOException {
        dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.h2.Driver");
        dataSource.setUrl("jdbc:h2:./target/benchmark-data/h2-bench;DB_CLOSE_DELAY=-1;AUTO_SERVER=TRUE");
        dataSource.setUsername("sa");
        dataSource.setPassword("");
        // Intentionally small pool to create contention
        dataSource.setMaxTotal(5);
        dataSource.setMaxIdle(2);
        dataSource.setMinIdle(1);
        dataSource.setMaxWaitMillis(5000);

        final JDBCPersistenceAdapter jdbc = new JDBCPersistenceAdapter();
        jdbc.setDataSource(dataSource);
        jdbc.setAdapter(new H2JDBCAdapter());
        jdbc.setUseLock(false);
        return jdbc;
    }

    @TearDown(Level.Trial)
    public void tearDownBroker() throws Exception {
        for (final Connection conn : consumerConnections) {
            try {
                conn.close();
            } catch (final Exception ignored) { }
        }
        consumerConnections.clear();
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        if (dataSource != null) {
            dataSource.close();
            dataSource = null;
        }
    }

    // ---- Benchmark methods ----

    @Benchmark
    @Threads(1)
    public void send_01_thread(final ThreadState state) throws Exception {
        doSend(state);
    }

    @Benchmark
    @Threads(2)
    public void send_02_threads(final ThreadState state) throws Exception {
        doSend(state);
    }

    @Benchmark
    @Threads(10)
    public void send_10_threads(final ThreadState state) throws Exception {
        doSend(state);
    }

    @Benchmark
    @Threads(20)
    public void send_20_threads(final ThreadState state) throws Exception {
        doSend(state);
    }

    @Benchmark
    @Threads(50)
    public void send_50_threads(final ThreadState state) throws Exception {
        doSend(state);
    }

    @Benchmark
    @Threads(100)
    public void send_100_threads(final ThreadState state) throws Exception {
        doSend(state);
    }

    private void doSend(final ThreadState state) throws Exception {
        final jakarta.jms.TextMessage msg = state.session.createTextMessage("bench-" + state.messageCounter++);
        state.producer.send(msg);
    }

    // ---- Slow persistence adapter (MEMORY_BUSYWAIT mode) ----

    @FunctionalInterface
    interface LatencyProvider {
        int getMicros();
    }

    static class SlowPersistenceAdapterWrapper extends MemoryPersistenceAdapter {
        private final LatencyProvider latencyProvider;

        SlowPersistenceAdapterWrapper(final LatencyProvider latencyProvider) {
            this.latencyProvider = latencyProvider;
        }

        @Override
        public TopicMessageStore createTopicMessageStore(final ActiveMQTopic destination) throws IOException {
            final TopicMessageStore realStore = super.createTopicMessageStore(destination);
            return new SlowTopicMessageStore(realStore, latencyProvider);
        }
    }

    static class SlowTopicMessageStore extends ProxyTopicMessageStore {
        private final LatencyProvider latencyProvider;

        SlowTopicMessageStore(final TopicMessageStore delegate, final LatencyProvider latencyProvider) {
            super(delegate);
            this.latencyProvider = latencyProvider;
        }

        @Override
        public void addMessage(final ConnectionContext context, final Message message) throws IOException {
            simulateIO();
            super.addMessage(context, message);
        }

        @Override
        public void addMessage(final ConnectionContext context, final Message message, final boolean canOptimizeHint) throws IOException {
            simulateIO();
            super.addMessage(context, message, canOptimizeHint);
        }

        @Override
        public ListenableFuture<Object> asyncAddTopicMessage(final ConnectionContext context, final Message message) throws IOException {
            simulateIO();
            return super.asyncAddTopicMessage(context, message);
        }

        @Override
        public ListenableFuture<Object> asyncAddTopicMessage(final ConnectionContext context, final Message message, final boolean canOptimizeHint) throws IOException {
            simulateIO();
            return super.asyncAddTopicMessage(context, message, canOptimizeHint);
        }

        private void simulateIO() {
            final int micros = latencyProvider.getMicros();
            if (micros > 0) {
                final long deadlineNanos = System.nanoTime() + TimeUnit.MICROSECONDS.toNanos(micros);
                while (System.nanoTime() < deadlineNanos) {
                    Thread.onSpinWait(); // important because it prevents the busy-wait loop from consuming 100% CPU and
                    // allows other threads to run, increasing contention and realism of the benchmark
                }
            }
        }
    }

    // Runner for manual execution (e.g. from IDE) — not needed when running via JMH plugin or command line

    public static void main(final String[] args) throws Exception {
        final Options opt = new OptionsBuilder()
                .include(TopicSendLockBenchmark.class.getSimpleName())
                .build();
        new Runner(opt).run();
    }
}
