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
package org.apache.activemq.perf;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.BufferPoolMXBean;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;

import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.Topic;

import com.sun.management.GarbageCollectionNotificationInfo;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Send latency of a varied-size topic fan-out through an nio:// connector, alongside direct memory, GC
 * activity and System.gc() pauses. Tune with system properties prefixed {@code nioPerf.}, e.g.
 * {@code -DnioPerf.durationSeconds=120 -DnioPerf.producers=8 -DnioPerf.sizeMix=30,40,22,8}.
 * Runs in the default test JVM and fails if oversize frames leave direct buffers behind; cap
 * -XX:MaxDirectMemorySize on the command line only to reproduce System.gc() stalls.
 */
public class NIOVariedSizeTopicLatencyTest {

    private static final Logger LOG = LoggerFactory.getLogger(NIOVariedSizeTopicLatencyTest.class);

    private static final int DURATION_SECONDS = Integer.getInteger("nioPerf.durationSeconds", 30);
    private static final int PRODUCERS = Integer.getInteger("nioPerf.producers", 4);
    private static final int CONSUMERS = Integer.getInteger("nioPerf.consumers", 4);
    private static final int RATE_PER_PRODUCER = Integer.getInteger("nioPerf.ratePerProducer", 100);
    private static final int HUGE_ONE_IN = Integer.getInteger("nioPerf.hugeOneIn", 500);
    private static final int HUGE_MIN = Integer.getInteger("nioPerf.hugeMinBytes", 8 * 1024 * 1024);
    private static final int HUGE_MAX = Integer.getInteger("nioPerf.hugeMaxBytes", 16 * 1024 * 1024);
    private static final int MAX_DIRECT_BUFFERS = 64;
    private static final long STALL_MILLIS = Long.getLong("nioPerf.stallMillis", 100);
    private static final long SEED = Long.getLong("nioPerf.seed", 2744L);
    private static final int[] SIZE_MIX = parseMix(System.getProperty("nioPerf.sizeMix", "30,40,22,8"));
    private static final int LIVE_HEAP_MB = Integer.getInteger("nioPerf.liveHeapMb",
            (int) Math.min(1536, Runtime.getRuntime().maxMemory() / (3 * 1024 * 1024)));

    private static final String[] BUCKETS = {"64B-8KB", "8KB-64KB", "64KB-1MB", "1MB-4MB", "huge"};
    private static final int[][] BUCKET_RANGES = {
        {64, 8 * 1024},
        {8 * 1024, 64 * 1024},
        {64 * 1024, 1024 * 1024},
        {1024 * 1024, 4 * 1024 * 1024},
        {HUGE_MIN, HUGE_MAX}
    };

    private BrokerService broker;
    private String connectUri;
    private Object[][] liveHeap;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.getSystemUsage().getMemoryUsage().setLimit(1024L * 1024 * 1024);
        TransportConnector connector = broker.addConnector("nio://localhost:0");
        broker.start();
        broker.waitUntilStarted();
        connectUri = connector.getPublishableConnectString().replace("nio://", "tcp://")
                + "?jms.alwaysSyncSend=true";
        liveHeap = allocateLiveHeap(LIVE_HEAP_MB);
    }

    @After
    public void tearDown() throws Exception {
        liveHeap = null;
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testVariedSizeTopicFanOut() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectUri);
        List<Connection> connections = new ArrayList<>();
        AtomicLong received = new AtomicLong();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        SystemGcListener systemGcs = new SystemGcListener();

        try {
            for (int i = 0; i < CONSUMERS; i++) {
                Connection connection = factory.createConnection();
                connections.add(connection);
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                MessageConsumer consumer = session.createConsumer(session.createTopic(topicName()));
                consumer.setMessageListener(message -> received.incrementAndGet());
                connection.start();
            }

            DirectMemorySampler sampler = new DirectMemorySampler();
            long gcCountBefore = gcCount();
            long gcMillisBefore = gcMillis();
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(DURATION_SECONDS);

            List<long[][]> results = new ArrayList<>();
            List<Thread> threads = new ArrayList<>();
            CountDownLatch start = new CountDownLatch(1);
            for (int p = 0; p < PRODUCERS; p++) {
                Connection connection = factory.createConnection();
                connections.add(connection);
                connection.start();
                Producer producer = new Producer(connection, new Random(SEED + p), deadline, start, failure);
                results.add(producer.latencies);
                Thread thread = new Thread(producer, "producer-" + p);
                threads.add(thread);
                thread.start();
            }

            systemGcs.register();
            sampler.start();
            start.countDown();
            for (Thread thread : threads) {
                thread.join();
            }
            sampler.stop();
            systemGcs.unregister();

            report(results, received.get(), sampler.peakBytes, sampler.peakCount, gcCount() - gcCountBefore, gcMillis() - gcMillisBefore, systemGcs);
            assertNull("producer failed: " + failure.get(), failure.get());
            assertTrue("no messages received", received.get() > 0);
            assertTrue("direct buffer count peaked at " + sampler.peakCount, sampler.peakCount < MAX_DIRECT_BUFFERS);
        } finally {
            systemGcs.unregister();
            for (Connection connection : connections) {
                try {
                    connection.close();
                } catch (Exception e) {
                    LOG.debug("close failed", e);
                }
            }
        }
    }

    private class Producer implements Runnable {
        final Connection connection;
        final Random random;
        final long deadline;
        final CountDownLatch start;
        final AtomicReference<Throwable> failure;
        final long[][] latencies = new long[BUCKETS.length][];
        final int[] counts = new int[BUCKETS.length];

        Producer(Connection connection, Random random, long deadline, CountDownLatch start, AtomicReference<Throwable> failure) {
            this.connection = connection;
            this.random = random;
            this.deadline = deadline;
            this.start = start;
            this.failure = failure;
            for (int i = 0; i < BUCKETS.length; i++) {
                latencies[i] = new long[1024];
            }
        }

        @Override
        public void run() {
            try {
                Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                Topic topic = session.createTopic(topicName());
                MessageProducer producer = session.createProducer(topic);
                producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
                byte[] payload = new byte[HUGE_MAX];
                long intervalNanos = TimeUnit.SECONDS.toNanos(1) / Math.max(1, RATE_PER_PRODUCER);
                start.await();
                long next = System.nanoTime();
                while (System.nanoTime() < deadline) {
                    int bucket = nextBucket(random);
                    int[] range = BUCKET_RANGES[bucket];
                    int size = range[0] + random.nextInt(Math.max(1, range[1] - range[0]));
                    BytesMessage message = session.createBytesMessage();
                    message.writeBytes(payload, 0, size);

                    long begin = System.nanoTime();
                    producer.send(message);
                    record(bucket, System.nanoTime() - begin);

                    next += intervalNanos;
                    long wait = next - System.nanoTime();
                    if (wait > 0) {
                        LockSupport.parkNanos(wait);
                    } else {
                        next = System.nanoTime();
                    }
                }
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
            for (int i = 0; i < BUCKETS.length; i++) {
                latencies[i] = Arrays.copyOf(latencies[i], counts[i]);
            }
        }

        private void record(int bucket, long nanos) {
            if (counts[bucket] == latencies[bucket].length) {
                latencies[bucket] = Arrays.copyOf(latencies[bucket], counts[bucket] * 2);
            }
            latencies[bucket][counts[bucket]++] = nanos;
        }
    }

    private static int nextBucket(Random random) {
        if (HUGE_ONE_IN > 0 && random.nextInt(HUGE_ONE_IN) == 0) {
            return BUCKETS.length - 1;
        }
        int total = 0;
        for (int weight : SIZE_MIX) {
            total += weight;
        }
        int pick = random.nextInt(total);
        for (int i = 0; i < SIZE_MIX.length; i++) {
            pick -= SIZE_MIX[i];
            if (pick < 0) {
                return i;
            }
        }
        return SIZE_MIX.length - 1;
    }

    private void report(List<long[][]> results, long received, long peakDirectBytes, long peakDirectCount, long gcCount, long gcMillis,
                        SystemGcListener systemGcs) {
        long[] all = new long[0];
        StringBuilder buckets = new StringBuilder();
        for (int b = 0; b < BUCKETS.length; b++) {
            long[] bucket = new long[0];
            for (long[][] producer : results) {
                bucket = concat(bucket, producer[b]);
            }
            Arrays.sort(bucket);
            all = concat(all, bucket);
            buckets.append(String.format("%n  %-9s %s", BUCKETS[b], ladder(bucket)));
        }
        Arrays.sort(all);
        long stalls = Arrays.stream(all).filter(n -> n > TimeUnit.MILLISECONDS.toNanos(STALL_MILLIS)).count();

        LOG.info(String.format("NIO varied-size topic fan-out: %ds, %d producers x %d msg/s, %d consumers, mix=%s, huge 1/%d (%d-%d B), seed=%d%n"
                + "  max heap=%d MB, max direct=%s, live heap=%d MB, JVM args=%s%n"
                + "  sent=%d received=%d stalls>%dms=%d%n"
                + "  send ms  %s%n"
                + "  per bucket:%s%n"
                + "  peak direct memory=%.1f MB, GC count=%d, GC time=%d ms, System.gc() pauses=%d (%d ms, max %d ms), direct buffers peak=%d",
                DURATION_SECONDS, PRODUCERS, RATE_PER_PRODUCER, CONSUMERS, Arrays.toString(SIZE_MIX), HUGE_ONE_IN, HUGE_MIN, HUGE_MAX, SEED,
                Runtime.getRuntime().maxMemory() / (1024 * 1024), maxDirectMemory(), LIVE_HEAP_MB,
                ManagementFactory.getRuntimeMXBean().getInputArguments(),
                all.length, received, STALL_MILLIS, stalls,
                ladder(all),
                buckets,
                peakDirectBytes / (1024.0 * 1024.0), gcCount, gcMillis,
                systemGcs.count.get(), systemGcs.totalMillis.get(), systemGcs.maxMillis.get(), peakDirectCount));
    }

    private static String ladder(long[] sorted) {
        if (sorted.length == 0) {
            return "n=0";
        }
        return String.format("n=%d p50=%.2f p75=%.2f p90=%.2f p99=%.2f p999=%.2f max=%.2f",
                sorted.length, pct(sorted, 0.50), pct(sorted, 0.75), pct(sorted, 0.90),
                pct(sorted, 0.99), pct(sorted, 0.999), sorted[sorted.length - 1] / 1e6);
    }

    private static double pct(long[] sorted, double quantile) {
        int index = (int) Math.ceil(quantile * sorted.length) - 1;
        return sorted[Math.max(0, Math.min(index, sorted.length - 1))] / 1e6;
    }

    private static long[] concat(long[] a, long[] b) {
        long[] result = Arrays.copyOf(a, a.length + b.length);
        System.arraycopy(b, 0, result, a.length, b.length);
        return result;
    }

    private static int[] parseMix(String mix) {
        int[] weights = Arrays.stream(mix.split(",")).map(String::trim).mapToInt(Integer::parseInt).toArray();
        if (weights.length != 4) {
            throw new IllegalArgumentException("nioPerf.sizeMix needs 4 weights, got " + mix);
        }
        return weights;
    }

    private static String topicName() {
        return NIOVariedSizeTopicLatencyTest.class.getSimpleName();
    }

    private static String maxDirectMemory() {
        return ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .filter(arg -> arg.startsWith("-XX:MaxDirectMemorySize="))
                .map(arg -> arg.substring("-XX:MaxDirectMemorySize=".length()))
                .reduce((first, last) -> last).orElse("heap");
    }

    private static Object[][] allocateLiveHeap(int megabytes) {
        int objectsPerChunk = 1 << 16;
        int chunks = (int) ((megabytes * 1024L * 1024L) / (40L * objectsPerChunk));
        Object[][] heap = new Object[chunks][];
        for (int c = 0; c < chunks; c++) {
            heap[c] = new Object[objectsPerChunk];
            for (int i = 0; i < objectsPerChunk; i++) {
                heap[c][i] = new long[2];
            }
        }
        return heap;
    }

    private static long gcCount() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionCount).filter(c -> c > 0).sum();
    }

    private static long gcMillis() {
        return ManagementFactory.getGarbageCollectorMXBeans().stream()
                .mapToLong(GarbageCollectorMXBean::getCollectionTime).filter(t -> t > 0).sum();
    }

    private static class SystemGcListener implements NotificationListener {
        final AtomicLong count = new AtomicLong();
        final AtomicLong totalMillis = new AtomicLong();
        final AtomicLong maxMillis = new AtomicLong();
        private final List<NotificationEmitter> emitters = new ArrayList<>();

        void register() {
            for (GarbageCollectorMXBean gc : ManagementFactory.getGarbageCollectorMXBeans()) {
                if (gc instanceof NotificationEmitter) {
                    NotificationEmitter emitter = (NotificationEmitter) gc;
                    emitter.addNotificationListener(this, null, null);
                    emitters.add(emitter);
                }
            }
        }

        void unregister() {
            for (NotificationEmitter emitter : emitters) {
                try {
                    emitter.removeNotificationListener(this);
                } catch (Exception ignored) {
                }
            }
            emitters.clear();
        }

        @Override
        public void handleNotification(Notification notification, Object handback) {
            if (!GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION.equals(notification.getType())) {
                return;
            }
            GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo.from((CompositeData) notification.getUserData());
            if ("System.gc()".equals(info.getGcCause())) {
                long millis = info.getGcInfo().getDuration();
                count.incrementAndGet();
                totalMillis.addAndGet(millis);
                maxMillis.accumulateAndGet(millis, Math::max);
            }
        }
    }

    private static class DirectMemorySampler implements Runnable {
        private final BufferPoolMXBean direct = ManagementFactory.getPlatformMXBeans(BufferPoolMXBean.class).stream()
                .filter(pool -> "direct".equals(pool.getName())).findFirst().orElseThrow();
        private final Thread thread = new Thread(this, "direct-memory-sampler");
        private volatile boolean running = true;
        volatile long peakBytes;
        volatile long peakCount;

        void start() {
            thread.setDaemon(true);
            thread.start();
        }

        void stop() throws InterruptedException {
            running = false;
            thread.join();
        }

        @Override
        public void run() {
            while (running) {
                peakBytes = Math.max(peakBytes, direct.getMemoryUsed());
                peakCount = Math.max(peakCount, direct.getCount());
                LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(50));
            }
        }
    }
}
