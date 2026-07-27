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
package org.apache.activemq;

import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.StampedLock;
import java.util.function.IntConsumer;

import org.apache.activemq.util.AtomicBitArrayBin;
import org.apache.activemq.util.BitArrayBin;
import org.apache.activemq.util.IdGenerator;
import org.junit.Test;

/**
 * Compares the throughput of message audit implementations under three
 * concurrency patterns:
 *
 * <ol>
 *   <li><b>Partitioned</b> &mdash; each thread owns a disjoint set of
 *       producers (no map or bin contention between threads)</li>
 *   <li><b>Shared</b> &mdash; all threads access all producers, each writing
 *       a distinct message slice (map read contention + per-bin lock
 *       contention)</li>
 *   <li><b>Mixed R/W</b> &mdash; audit is pre-populated, then all threads
 *       interleave duplicate re-checks (reads) with new inserts (writes)
 *       across all producers (the most realistic broker workload)</li>
 * </ol>
 *
 * Strategies under test:
 * <ol>
 *   <li><b>sync+LRU</b> &mdash; upstream {@link ActiveMQMessageAudit}</li>
 *   <li><b>CHM+Atomic</b> &mdash; {@link ConcurrentMessageAudit}</li>
 *   <li><b>Caffeine</b> &mdash; {@link CaffeineMessageAudit}</li>
 *   <li><b>SkipList</b> &mdash; {@link ConcurrentSkipListMap} + per-bin sync</li>
 *   <li><b>RWLock</b> &mdash; {@link java.util.HashMap} + {@link ReentrantReadWriteLock}</li>
 *   <li><b>Stamped</b> &mdash; {@link java.util.HashMap} + {@link StampedLock}</li>
 * </ol>
 */
public class MessageAuditPerformanceTest {

    private static final int WARMUP_ITERATIONS = 3;
    private static final int MEASURE_ITERATIONS = 5;
    private static final int MESSAGES_PER_PRODUCER = 5_000;

    private static final int[] THREAD_COUNTS = {1, 2, 4, 8};
    private static final int[] PRODUCER_COUNTS = {1, 8, 16, 64, 128};

    private static final String[] NAMES = {"sync+LRU", "CHM+Atomic", "Caffeine", "SkipList", "RWLock", "Stamped"};
    private static final AuditFactory[] FACTORIES = {
        MessageAuditPerformanceTest::createSyncLru,
        MessageAuditPerformanceTest::createConcurrentAudit,
        MessageAuditPerformanceTest::createCaffeineAudit,
        MessageAuditPerformanceTest::createSkipList,
        MessageAuditPerformanceTest::createReadWriteLock,
        MessageAuditPerformanceTest::createStampedLock
    };

    @FunctionalInterface
    private interface StringAudit {
        boolean isDuplicate(String id);
    }

    @FunctionalInterface
    private interface AuditFactory {
        StringAudit create(int auditDepth, int producers);
    }

    @FunctionalInterface
    private interface BenchmarkRunner {
        long run(StringAudit fn, String[][] ids, String[][] extraIds,
                 int threads, int producers) throws Exception;
    }

    // ========================================================================
    // Benchmark scenarios
    // ========================================================================

    @Test
    public void comparePartitioned() throws Exception {
        printBenchmark("PARTITIONED — producers split across threads, no cross-thread contention",
                (fn, ids, extra, threads, producers) ->
                        runPartitioned(fn, ids, threads, producers));
    }

    @Test
    public void compareSharedContention() throws Exception {
        printBenchmark("SHARED — all threads hit all producers, per-bin lock contention",
                (fn, ids, extra, threads, producers) ->
                        runShared(fn, ids, threads, producers));
    }

    @Test
    public void compareMixedReadWrite() throws Exception {
        printBenchmark("MIXED R/W — pre-populated, 50/50 duplicate re-checks and new inserts",
                (fn, ids, extra, threads, producers) ->
                        runMixed(fn, ids, extra, threads, producers));
    }

    @Test
    public void compareOptimizationLayers() throws Exception {
        String[] layerNames = {"sync+LRU", "CHM+syncBin", "CHM+AtomicBin"};
        AuditFactory[] layerFactories = {
            MessageAuditPerformanceTest::createSyncLru,
            MessageAuditPerformanceTest::createChmSyncBin,
            MessageAuditPerformanceTest::createConcurrentAudit
        };

        BenchmarkRunner[] runners = {
            (fn, ids, extra, threads, producers) ->
                    runPartitioned(fn, ids, threads, producers),
            (fn, ids, extra, threads, producers) ->
                    runShared(fn, ids, threads, producers),
            (fn, ids, extra, threads, producers) ->
                    runMixed(fn, ids, extra, threads, producers)
        };
        String[] scenarioNames = {
            "PARTITIONED — producers split across threads, no cross-thread contention",
            "SHARED — all threads hit all producers, per-bin lock contention",
            "MIXED R/W — pre-populated, 50/50 duplicate re-checks and new inserts"
        };

        System.out.println();
        System.out.println("==========================================================================");
        System.out.println("  OPTIMIZATION LAYER COMPARISON");
        System.out.println("  Layer 1: sync+LRU        — LinkedHashMap + synchronized(this) + BitArrayBin");
        System.out.println("  Layer 2: CHM+syncBin     — ConcurrentHashMap + per-bin synchronized + BitArrayBin");
        System.out.println("  Layer 3: CHM+AtomicBin   — ConcurrentHashMap + AtomicBitArrayBin (lock-free)");
        System.out.println("==========================================================================");

        for (var s = 0; s < runners.length; s++) {
            System.out.println();
            System.out.println("=== " + scenarioNames[s] + " ===");

            for (var producers : PRODUCER_COUNTS) {
                var totalMessages = producers * MESSAGES_PER_PRODUCER;
                var ids = generateStringIds(producers, MESSAGES_PER_PRODUCER);
                var extraIds = generateStringIds(producers, MESSAGES_PER_PRODUCER);

                System.out.println();
                System.out.printf("--- %d producers, %,d messages ---%n", producers, totalMessages);
                System.out.printf("%-8s", "Threads");
                for (var name : layerNames) {
                    System.out.printf(" | %14s", name);
                }
                System.out.printf(" | %10s | %10s%n", "Map gain", "Bin gain");
                System.out.print("--------");
                for (var i = 0; i < layerNames.length; i++) {
                    System.out.print("-+-" + "--------------");
                }
                System.out.print("-+-----------+-----------");
                System.out.println();

                for (var threads : THREAD_COUNTS) {
                    var medians = new long[layerFactories.length];
                    for (var f = 0; f < layerFactories.length; f++) {
                        medians[f] = benchmarkWith(layerFactories[f], runners[s],
                                ids, extraIds, threads, producers);
                    }

                    var best = Long.MAX_VALUE;
                    for (var m : medians) {
                        if (m < best) best = m;
                    }

                    System.out.printf("%8d", threads);
                    for (var f = 0; f < layerFactories.length; f++) {
                        var ms = medians[f] / 1_000_000.0;
                        var marker = medians[f] == best ? " *" : "";
                        System.out.printf(" | %11.2f ms%s", ms, marker);
                    }

                    var mapSpeedup = (double) medians[0] / medians[1];
                    var binSpeedup = (double) medians[1] / medians[2];
                    System.out.printf(" | %9.1fx | %9.1fx", mapSpeedup, binSpeedup);
                    System.out.println();
                }
            }
        }
        System.out.println();
        System.out.println("  * = fastest for that row");
        System.out.println("  Map gain  = sync+LRU / CHM+syncBin  (improvement from concurrent map)");
        System.out.println("  Bin gain  = CHM+syncBin / CHM+AtomicBin  (improvement from lock-free bits)");
        System.out.println();
    }

    @Test
    public void compareEvictionOverhead() throws Exception {
        System.out.println();
        System.out.println("=== EVICTION — producers exceed max, measures eviction overhead ===");

        String[] evictionNames = {"sync+LRU", "CHM+Atomic", "Caffeine"};
        AuditFactory[] evictionFactories = {
            MessageAuditPerformanceTest::createSyncLru,
            MessageAuditPerformanceTest::createConcurrentAudit,
            MessageAuditPerformanceTest::createCaffeineAudit
        };

        int[] maxProducerLimits = {16, 32, 64};
        var producerMultiplier = 4;

        for (var maxProducers : maxProducerLimits) {
            var actualProducers = maxProducers * producerMultiplier;
            var totalMessages = actualProducers * MESSAGES_PER_PRODUCER;
            var ids = generateStringIds(actualProducers, MESSAGES_PER_PRODUCER);
            var extraIds = generateStringIds(actualProducers, MESSAGES_PER_PRODUCER);

            System.out.println();
            System.out.printf("--- max=%d, actual=%d producers, %,d messages ---%n",
                    maxProducers, actualProducers, totalMessages);
            System.out.printf("%-8s", "Threads");
            for (var name : evictionNames) {
                System.out.printf(" | %12s", name);
            }
            System.out.println();
            System.out.print("--------");
            for (var i = 0; i < evictionNames.length; i++) {
                System.out.print("-+-" + "------------");
            }
            System.out.println();

            for (var threads : THREAD_COUNTS) {
                var medians = new long[evictionFactories.length];
                for (var f = 0; f < evictionFactories.length; f++) {
                    medians[f] = benchmarkWith(evictionFactories[f],
                            (fn, idArr, extra, t, p) -> runShared(fn, idArr, t, p),
                            ids, extraIds, threads, actualProducers,
                            maxProducers);
                }

                var best = Long.MAX_VALUE;
                for (var m : medians) {
                    if (m < best) best = m;
                }

                System.out.printf("%8d", threads);
                for (var f = 0; f < evictionFactories.length; f++) {
                    var ms = medians[f] / 1_000_000.0;
                    var marker = medians[f] == best ? " *" : "";
                    System.out.printf(" | %9.2f ms%s", ms, marker);
                }
                System.out.println();
            }
        }
        System.out.println();
        System.out.println("  * = fastest for that row");
        System.out.println();
    }

    // ========================================================================
    // Correctness
    // ========================================================================

    @Test
    public void verifyCorrectnessEquivalence() {
        var producers = 32;
        var messagesPerProducer = 1_000;
        var ids = generateStringIds(producers, messagesPerProducer);

        var lru = new ActiveMQMessageAudit(2048, producers);
        var concurrent = new ConcurrentMessageAudit(2048, producers);
        var caffeine = new CaffeineMessageAudit(2048, producers);

        for (var p = 0; p < producers; p++) {
            for (var m = 0; m < messagesPerProducer; m++) {
                var lruResult = lru.isDuplicate(ids[p][m]);
                var concurrentResult = concurrent.isDuplicate(ids[p][m]);
                var caffeineResult = caffeine.isDuplicate(ids[p][m]);
                assertEquals("ConcurrentMessageAudit mismatch at producer " + p + " message " + m,
                        lruResult, concurrentResult);
                assertEquals("CaffeineMessageAudit mismatch at producer " + p + " message " + m,
                        lruResult, caffeineResult);
            }
        }

        for (var p = 0; p < producers; p++) {
            for (var m = 0; m < messagesPerProducer; m++) {
                assertTrue("Should be duplicate on second pass (LRU)",
                        lru.isDuplicate(ids[p][m]));
                assertTrue("Should be duplicate on second pass (Concurrent)",
                        concurrent.isDuplicate(ids[p][m]));
                assertTrue("Should be duplicate on second pass (Caffeine)",
                        caffeine.isDuplicate(ids[p][m]));
            }
        }
    }

    @Test
    public void verifyCorrectnessUnderConcurrency() throws Exception {
        var producers = 16;
        var messagesPerProducer = 2_000;
        var threads = 4;
        var ids = generateStringIds(producers, messagesPerProducer);
        var producersPerThread = producers / threads;

        var concurrentAudit = new ConcurrentMessageAudit(2048, producers);
        var duplicateCount = new AtomicInteger(0);
        runConcurrentPass(concurrentAudit::isDuplicate, ids, threads, producersPerThread, duplicateCount);
        assertEquals("ConcurrentMessageAudit: first pass should have zero duplicates", 0, duplicateCount.get());

        var duplicateCount2 = new AtomicInteger(0);
        runConcurrentPass(concurrentAudit::isDuplicate, ids, threads, producersPerThread, duplicateCount2);
        var expectedDuplicates = producers * messagesPerProducer;
        assertEquals("ConcurrentMessageAudit: second pass should all be duplicates",
                expectedDuplicates, duplicateCount2.get());

        var caffeineAudit = new CaffeineMessageAudit(2048, producers);
        var caffDupCount = new AtomicInteger(0);
        runConcurrentPass(caffeineAudit::isDuplicate, ids, threads, producersPerThread, caffDupCount);
        assertEquals("CaffeineMessageAudit: first pass should have zero duplicates", 0, caffDupCount.get());

        var caffDupCount2 = new AtomicInteger(0);
        runConcurrentPass(caffeineAudit::isDuplicate, ids, threads, producersPerThread, caffDupCount2);
        assertEquals("CaffeineMessageAudit: second pass should all be duplicates",
                expectedDuplicates, caffDupCount2.get());
    }

    @Test
    public void verifyEvictionBehavior() {
        var maxProducers = 16;
        var actualProducers = 64;
        var messagesPerProducer = 100;
        var ids = generateStringIds(actualProducers, messagesPerProducer);

        // --- ActiveMQMessageAudit (LRU eviction) ---
        var lru = new ActiveMQMessageAudit(2048, maxProducers);
        for (var p = 0; p < actualProducers; p++) {
            for (var m = 0; m < messagesPerProducer; m++) {
                lru.isDuplicate(ids[p][m]);
            }
        }
        // LRU keeps the most recently accessed producers
        var recentStart = actualProducers - maxProducers;
        for (var p = recentStart; p < actualProducers; p++) {
            assertTrue("LRU should detect duplicate for recent producer " + p,
                    lru.isDuplicate(ids[p][0]));
        }

        // --- ConcurrentMessageAudit (bounded, hash-order eviction) ---
        var concurrent = new ConcurrentMessageAudit(2048, maxProducers);
        for (var p = 0; p < actualProducers; p++) {
            for (var m = 0; m < messagesPerProducer; m++) {
                concurrent.isDuplicate(ids[p][m]);
            }
        }
        assertTrue("Concurrent producer count should be bounded",
                concurrent.getProducerCount() <= maxProducers);
        var concurrentDuplicatesDetected = 0;
        for (var p = 0; p < actualProducers; p++) {
            if (concurrent.isDuplicate(ids[p][0])) {
                concurrentDuplicatesDetected++;
            }
        }
        assertTrue("Concurrent should detect some duplicates among surviving producers",
                concurrentDuplicatesDetected > 0);

        // --- CaffeineMessageAudit (TinyLfu eviction) ---
        var caffeine = new CaffeineMessageAudit(2048, maxProducers);
        for (var p = 0; p < actualProducers; p++) {
            for (var m = 0; m < messagesPerProducer; m++) {
                caffeine.isDuplicate(ids[p][m]);
            }
        }
        // Caffeine eviction is asynchronous; cleanUp forces pending evictions
        var caffeineCount = caffeine.getProducerCount();
        assertTrue("Caffeine producer count should be bounded (was " + caffeineCount + ")",
                caffeineCount <= maxProducers * 2);
        var freshIds = generateStringIds(4, messagesPerProducer);
        for (var p = 0; p < 4; p++) {
            assertFalse("Fresh producer should not be a duplicate",
                    caffeine.isDuplicate(freshIds[p][0]));
        }
    }

    @Test
    public void verifySetMaximumDuringTrafficPreservesState() throws Exception {
        var producers = 8;
        var messagesPerProducer = 2_000;

        var concurrent = new ConcurrentMessageAudit(2048, 64);
        runSetMaxDuringTraffic("ConcurrentMessageAudit", concurrent::isDuplicate,
                concurrent::setMaximumNumberOfProducersToTrack,
                generateStringIds(producers, messagesPerProducer));

        var caffeine = new CaffeineMessageAudit(2048, 64);
        runSetMaxDuringTraffic("CaffeineMessageAudit", caffeine::isDuplicate,
                caffeine::setMaximumNumberOfProducersToTrack,
                generateStringIds(producers, messagesPerProducer));
    }

    /**
     * Churns setMaximumNumberOfProducersToTrack between 32 and 64 while ids
     * are inserted. With only 8 producers no eviction is ever legitimate, so
     * every id must be detected as a duplicate afterwards — any miss means
     * the resize lost audit state.
     */
    private void runSetMaxDuringTraffic(String label, StringAudit fn, IntConsumer setMax,
                                        String[][] ids) throws Exception {
        var producers = ids.length;
        var threads = 2;
        var stop = new AtomicBoolean(false);
        var resizer = new Thread(() -> {
            var flip = false;
            while (!stop.get()) {
                setMax.accept(flip ? 32 : 64);
                flip = !flip;
            }
        });
        resizer.start();
        try {
            var firstPass = new AtomicInteger();
            runConcurrentPass(fn, ids, threads, producers / threads, firstPass);
        } finally {
            stop.set(true);
            resizer.join();
        }

        var duplicates = new AtomicInteger();
        runConcurrentPass(fn, ids, threads, producers / threads, duplicates);
        assertEquals(label + ": resize during traffic must not lose audit state",
                producers * ids[0].length, duplicates.get());
    }

    // ========================================================================
    // Implementations under test
    // ========================================================================

    private static StringAudit createSyncLru(int auditDepth, int producers) {
        var audit = new ActiveMQMessageAudit(auditDepth, producers);
        return audit::isDuplicate;
    }

    private static StringAudit createConcurrentAudit(int auditDepth, int producers) {
        var audit = new ConcurrentMessageAudit(auditDepth, producers);
        return audit::isDuplicate;
    }

    private static StringAudit createCaffeineAudit(int auditDepth, int producers) {
        var audit = new CaffeineMessageAudit(auditDepth, producers);
        return audit::isDuplicate;
    }

    private static StringAudit createSkipList(int auditDepth, int producers) {
        var map = new ConcurrentSkipListMap<String, BitArrayBin>();
        return id -> {
            var seed = IdGenerator.getSeedFromId(id);
            if (seed == null) return false;
            var bab = map.computeIfAbsent(seed, k -> new BitArrayBin(auditDepth));
            var index = IdGenerator.getSequenceFromId(id);
            if (index >= 0) {
                synchronized (bab) {
                    return bab.setBit(index, true);
                }
            }
            return false;
        };
    }

    private static StringAudit createReadWriteLock(int auditDepth, int producers) {
        var map = new HashMap<String, BitArrayBin>(producers);
        var rwLock = new ReentrantReadWriteLock();
        return id -> {
            var seed = IdGenerator.getSeedFromId(id);
            if (seed == null) return false;

            BitArrayBin bab;
            rwLock.readLock().lock();
            try {
                bab = map.get(seed);
            } finally {
                rwLock.readLock().unlock();
            }
            if (bab == null) {
                rwLock.writeLock().lock();
                try {
                    bab = map.get(seed);
                    if (bab == null) {
                        bab = new BitArrayBin(auditDepth);
                        map.put(seed, bab);
                    }
                } finally {
                    rwLock.writeLock().unlock();
                }
            }

            var index = IdGenerator.getSequenceFromId(id);
            if (index >= 0) {
                synchronized (bab) {
                    return bab.setBit(index, true);
                }
            }
            return false;
        };
    }

    private static StringAudit createStampedLock(int auditDepth, int producers) {
        var map = new HashMap<String, BitArrayBin>(producers);
        var sl = new StampedLock();
        return id -> {
            var seed = IdGenerator.getSeedFromId(id);
            if (seed == null) return false;

            BitArrayBin bab;
            var stamp = sl.tryOptimisticRead();
            bab = map.get(seed);
            if (!sl.validate(stamp)) {
                stamp = sl.readLock();
                try {
                    bab = map.get(seed);
                } finally {
                    sl.unlockRead(stamp);
                }
            }
            if (bab == null) {
                stamp = sl.writeLock();
                try {
                    bab = map.get(seed);
                    if (bab == null) {
                        bab = new BitArrayBin(auditDepth);
                        map.put(seed, bab);
                    }
                } finally {
                    sl.unlockWrite(stamp);
                }
            }

            var index = IdGenerator.getSequenceFromId(id);
            if (index >= 0) {
                synchronized (bab) {
                    return bab.setBit(index, true);
                }
            }
            return false;
        };
    }

    private static StringAudit createChmSyncBin(int auditDepth, int producers) {
        var map = new ConcurrentHashMap<String, BitArrayBin>(producers);
        return id -> {
            var seed = IdGenerator.getSeedFromId(id);
            if (seed == null) return false;
            var bab = map.computeIfAbsent(seed, k -> new BitArrayBin(auditDepth));
            var index = IdGenerator.getSequenceFromId(id);
            if (index >= 0) {
                synchronized (bab) {
                    return bab.setBit(index, true);
                }
            }
            return false;
        };
    }

    // ========================================================================
    // Benchmark runners
    // ========================================================================

    /**
     * Each thread owns a disjoint slice of producers. No two threads ever
     * look up the same map key or touch the same BitArrayBin.
     */
    private long runPartitioned(StringAudit fn, String[][] ids,
                                int threads, int producers) throws Exception {
        if (threads == 1) {
            return runSingleThread(fn, ids, producers);
        }

        var barrier = new CyclicBarrier(threads + 1);
        var done = new CountDownLatch(threads);
        var perThread = producers / threads;
        var remainder = producers % threads;

        for (var t = 0; t < threads; t++) {
            final var startP = t * perThread + Math.min(t, remainder);
            final var count = perThread + (t < remainder ? 1 : 0);
            new Thread(() -> {
                try {
                    barrier.await();
                    for (var p = startP; p < startP + count; p++) {
                        for (var m = 0; m < ids[p].length; m++) {
                            fn.isDuplicate(ids[p][m]);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }).start();
        }

        var start = System.nanoTime();
        barrier.await();
        done.await();
        return System.nanoTime() - start;
    }

    /**
     * All threads iterate over ALL producers. Each thread writes a distinct
     * slice of messages per producer, so no duplicate IDs, but the map is
     * read-contended and every producer's BitArrayBin is lock-contended.
     */
    private long runShared(StringAudit fn, String[][] ids,
                           int threads, int producers) throws Exception {
        if (threads == 1) {
            return runSingleThread(fn, ids, producers);
        }

        var msgsPerProducer = ids[0].length;
        var chunk = msgsPerProducer / threads;

        var barrier = new CyclicBarrier(threads + 1);
        var done = new CountDownLatch(threads);

        for (var t = 0; t < threads; t++) {
            final var msgStart = t * chunk;
            final var msgEnd = (t == threads - 1) ? msgsPerProducer : (t + 1) * chunk;
            new Thread(() -> {
                try {
                    barrier.await();
                    for (var p = 0; p < producers; p++) {
                        for (var m = msgStart; m < msgEnd; m++) {
                            fn.isDuplicate(ids[p][m]);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }).start();
        }

        var start = System.nanoTime();
        barrier.await();
        done.await();
        return System.nanoTime() - start;
    }

    /**
     * Pre-populate the audit with {@code ids}, then all threads interleave
     * duplicate re-checks ({@code ids} &mdash; reads) with new inserts
     * ({@code extraIds} &mdash; writes) across all producers.
     */
    private long runMixed(StringAudit fn, String[][] ids, String[][] extraIds,
                          int threads, int producers) throws Exception {
        for (var p = 0; p < producers; p++) {
            for (var m = 0; m < ids[p].length; m++) {
                fn.isDuplicate(ids[p][m]);
            }
        }

        if (threads == 1) {
            var start = System.nanoTime();
            for (var p = 0; p < producers; p++) {
                for (var m = 0; m < ids[p].length; m++) {
                    fn.isDuplicate(ids[p][m]);
                    fn.isDuplicate(extraIds[p][m]);
                }
            }
            return System.nanoTime() - start;
        }

        var msgsPerProducer = ids[0].length;
        var chunk = msgsPerProducer / threads;

        var barrier = new CyclicBarrier(threads + 1);
        var done = new CountDownLatch(threads);

        for (var t = 0; t < threads; t++) {
            final var msgStart = t * chunk;
            final var msgEnd = (t == threads - 1) ? msgsPerProducer : (t + 1) * chunk;
            new Thread(() -> {
                try {
                    barrier.await();
                    for (var p = 0; p < producers; p++) {
                        for (var m = msgStart; m < msgEnd; m++) {
                            fn.isDuplicate(ids[p][m]);
                            fn.isDuplicate(extraIds[p][m]);
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }).start();
        }

        var start = System.nanoTime();
        barrier.await();
        done.await();
        return System.nanoTime() - start;
    }

    private long runSingleThread(StringAudit fn, String[][] ids, int producers) {
        var start = System.nanoTime();
        for (var p = 0; p < producers; p++) {
            for (var m = 0; m < ids[p].length; m++) {
                fn.isDuplicate(ids[p][m]);
            }
        }
        return System.nanoTime() - start;
    }

    // ========================================================================
    // Benchmark harness + table printing
    // ========================================================================

    private void printBenchmark(String title, BenchmarkRunner runner) throws Exception {
        System.out.println();
        System.out.println("=== " + title + " ===");

        for (var producers : PRODUCER_COUNTS) {
            var totalMessages = producers * MESSAGES_PER_PRODUCER;
            var ids = generateStringIds(producers, MESSAGES_PER_PRODUCER);
            var extraIds = generateStringIds(producers, MESSAGES_PER_PRODUCER);

            System.out.println();
            System.out.printf("--- %d producers, %,d messages ---%n", producers, totalMessages);
            System.out.printf("%-8s", "Threads");
            for (var name : NAMES) {
                System.out.printf(" | %12s", name);
            }
            System.out.println();
            System.out.print("--------");
            for (var i = 0; i < NAMES.length; i++) {
                System.out.print("-+-" + "------------");
            }
            System.out.println();

            for (var threads : THREAD_COUNTS) {
                var medians = new long[FACTORIES.length];
                for (var f = 0; f < FACTORIES.length; f++) {
                    medians[f] = benchmarkWith(FACTORIES[f], runner, ids, extraIds, threads, producers);
                }

                var best = Long.MAX_VALUE;
                for (var m : medians) {
                    if (m < best) best = m;
                }

                System.out.printf("%8d", threads);
                for (var f = 0; f < FACTORIES.length; f++) {
                    var ms = medians[f] / 1_000_000.0;
                    var marker = medians[f] == best ? " *" : "";
                    System.out.printf(" | %9.2f ms%s", ms, marker);
                }
                System.out.println();
            }
        }
        System.out.println();
        System.out.println("  * = fastest for that row");
        System.out.println();
    }

    private long benchmarkWith(AuditFactory factory, BenchmarkRunner runner,
                                String[][] ids, String[][] extraIds,
                                int threads, int producers) throws Exception {
        return benchmarkWith(factory, runner, ids, extraIds, threads, producers, producers);
    }

    private long benchmarkWith(AuditFactory factory, BenchmarkRunner runner,
                                String[][] ids, String[][] extraIds,
                                int threads, int producers,
                                int maxProducers) throws Exception {
        var times = new long[WARMUP_ITERATIONS + MEASURE_ITERATIONS];
        for (var i = 0; i < times.length; i++) {
            var audit = factory.create(2048, maxProducers);
            times[i] = runner.run(audit, ids, extraIds, threads, producers);
        }
        return median(times, WARMUP_ITERATIONS);
    }

    private void runConcurrentPass(StringAudit fn, String[][] ids,
                                   int threads, int producersPerThread,
                                   AtomicInteger counter) throws Exception {
        var barrier = new CyclicBarrier(threads);
        var done = new CountDownLatch(threads);
        for (var t = 0; t < threads; t++) {
            final var startProducer = t * producersPerThread;
            final var endProducer = startProducer + producersPerThread;
            new Thread(() -> {
                try {
                    barrier.await();
                    for (var p = startProducer; p < endProducer; p++) {
                        for (var m = 0; m < ids[p].length; m++) {
                            if (fn.isDuplicate(ids[p][m])) {
                                counter.incrementAndGet();
                            }
                        }
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    done.countDown();
                }
            }).start();
        }
        done.await();
    }

    // ========================================================================
    // Helpers
    // ========================================================================

    private String[][] generateStringIds(int producers, int messagesPerProducer) {
        var ids = new String[producers][messagesPerProducer];
        for (var p = 0; p < producers; p++) {
            var gen = new IdGenerator();
            for (var m = 0; m < messagesPerProducer; m++) {
                ids[p][m] = gen.generateId();
            }
        }
        return ids;
    }

    private long median(long[] times, int skipFirst) {
        var measured = Arrays.copyOfRange(times, skipFirst, times.length);
        Arrays.sort(measured);
        return measured[measured.length / 2];
    }
}
