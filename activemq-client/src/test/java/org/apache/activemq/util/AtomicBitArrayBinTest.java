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
package org.apache.activemq.util;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.Test;

public class AtomicBitArrayBinTest {

    @Test
    public void testSetAndGetBit() {
        var bin = new AtomicBitArrayBin(512);

        assertFalse("first set should return false", bin.setBit(0, true));
        assertTrue("second set should return true (duplicate)", bin.setBit(0, true));

        assertFalse("first set of 1 should return false", bin.setBit(1, true));
        assertTrue("bit 0 should still be set", bin.getBit(0));
        assertTrue("bit 1 should be set", bin.getBit(1));
        assertFalse("bit 2 should not be set", bin.getBit(2));
    }

    @Test
    public void testClearBit() {
        var bin = new AtomicBitArrayBin(512);

        bin.setBit(10, true);
        assertTrue("bit should be set", bin.getBit(10));

        assertTrue("clear should return true (was set)", bin.setBit(10, false));
        assertFalse("bit should be cleared", bin.getBit(10));

        assertFalse("clear again should return false (was already clear)", bin.setBit(10, false));
    }

    @Test
    public void testSequentialBits() {
        var bin = new AtomicBitArrayBin(2048);

        for (int i = 0; i < 1000; i++) {
            assertFalse("first set of " + i + " should return false", bin.setBit(i, true));
        }
        for (int i = 0; i < 1000; i++) {
            assertTrue("bit " + i + " should be set", bin.getBit(i));
            assertTrue("duplicate set of " + i + " should return true", bin.setBit(i, true));
        }
    }

    @Test
    public void testWindowSlide() {
        var windowSize = 128;
        var bin = new AtomicBitArrayBin(windowSize);

        bin.setBit(0, true);
        bin.setBit(63, true);
        assertTrue(bin.getBit(0));
        assertTrue(bin.getBit(63));

        // Set a bit far beyond the window to force advancement
        var farIndex = windowSize + 128;
        assertFalse(bin.setBit(farIndex, true));
        assertTrue(bin.getBit(farIndex));

        // Original bits should now be "behind window" — getBit returns true
        assertTrue("behind-window bits return true", bin.getBit(0));
        assertTrue("behind-window bits return true", bin.getBit(63));
    }

    @Test
    public void testBehindWindowSetIsNoOpAndGetReturnsTrue() {
        var bin = new AtomicBitArrayBin(128);

        bin.setBit(0, true);
        // Advance window far past index 0
        bin.setBit(500, true);

        // Behind-window setBit is a no-op returning false (BitArrayBin parity):
        // a late message is accepted rather than dropped as a duplicate
        assertFalse("behind-window setBit should be a no-op returning false",
                bin.setBit(0, true));

        // Behind-window getBit still reports the range as seen
        assertTrue("behind-window getBit should return true", bin.getBit(0));
    }

    @Test
    public void testCrossBoundaryBits() {
        var bin = new AtomicBitArrayBin(512);

        // Set bits across multiple 64-bit slot boundaries
        for (int i = 0; i < 256; i += 63) {
            assertFalse(bin.setBit(i, true));
        }
        for (int i = 0; i < 256; i += 63) {
            assertTrue("bit " + i + " should be set", bin.getBit(i));
        }
    }

    @Test
    public void testIsInOrder() {
        var bin = new AtomicBitArrayBin(512);

        assertTrue("first message is always in order", bin.isInOrder(0));
        assertTrue("sequential is in order", bin.isInOrder(1));
        assertTrue("sequential is in order", bin.isInOrder(2));
        assertFalse("gap breaks order", bin.isInOrder(5));
        assertTrue("next after gap is in order", bin.isInOrder(6));
    }

    @Test
    public void testGetLastSetIndex() {
        var bin = new AtomicBitArrayBin(512);

        assertEquals(-1, bin.getLastSetIndex());

        bin.setBit(10, true);
        assertEquals(10, bin.getLastSetIndex());

        bin.setBit(100, true);
        assertEquals(100, bin.getLastSetIndex());

        bin.setBit(50, true);
        assertEquals("last set index should be highest", 100, bin.getLastSetIndex());
    }

    @Test
    public void testLargeSequenceNumbers() {
        var bin = new AtomicBitArrayBin(2048);

        var base = 1_000_000L;
        for (var i = base; i < base + 500; i++) {
            assertFalse(bin.setBit(i, true));
        }
        for (var i = base; i < base + 500; i++) {
            assertTrue(bin.getBit(i));
            assertTrue(bin.setBit(i, true));
        }
    }

    @Test
    public void testEquivalenceWithBitArrayBin() {
        var windowSize = 2048;
        var original = new BitArrayBin(windowSize);
        var atomic = new AtomicBitArrayBin(windowSize);

        // Sequential inserts
        for (var i = 0; i < 3000; i++) {
            var origResult = original.setBit(i, true);
            var atomicResult = atomic.setBit(i, true);
            assertEquals("setBit(" + i + ") mismatch", origResult, atomicResult);
        }

        // Duplicate checks on values still in window
        for (var i = 2000; i < 3000; i++) {
            var origResult = original.setBit(i, true);
            var atomicResult = atomic.setBit(i, true);
            assertEquals("duplicate setBit(" + i + ") mismatch", origResult, atomicResult);
        }
    }

    @Test
    public void testEquivalenceWithBitArrayBinSparseIndices() {
        var windowSize = 512;
        var original = new BitArrayBin(windowSize);
        var atomic = new AtomicBitArrayBin(windowSize);

        // Sparse indices within a single window
        int[] indices = {0, 1, 63, 64, 65, 127, 128, 200, 300, 400, 511};
        for (var idx : indices) {
            assertEquals("first set " + idx, original.setBit(idx, true), atomic.setBit(idx, true));
        }
        for (var idx : indices) {
            assertEquals("dup set " + idx, original.setBit(idx, true), atomic.setBit(idx, true));
        }
    }

    @Test
    public void testConcurrentSetsNoDuplicateLoss() throws Exception {
        var threadCount = 8;
        var messagesPerThread = 10_000;
        var bin = new AtomicBitArrayBin(threadCount * messagesPerThread);

        var executor = Executors.newFixedThreadPool(threadCount);
        var barrier = new CyclicBarrier(threadCount);
        var duplicateCount = new AtomicInteger(0);

        var futures = new ArrayList<Future<?>>();
        for (var t = 0; t < threadCount; t++) {
            var threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (var i = 0; i < messagesPerThread; i++) {
                    // Each thread gets unique indices (no overlap)
                    var index = (long) threadId * messagesPerThread + i;
                    var wasDuplicate = bin.setBit(index, true);
                    if (wasDuplicate) {
                        duplicateCount.incrementAndGet();
                    }
                }
            }));
        }

        for (var f : futures) f.get();
        executor.shutdown();

        assertEquals("no false duplicates with disjoint indices", 0, duplicateCount.get());

        // Verify all bits are set
        for (var t = 0; t < threadCount; t++) {
            for (var i = 0; i < messagesPerThread; i++) {
                var index = (long) t * messagesPerThread + i;
                assertTrue("bit " + index + " should be set", bin.getBit(index));
            }
        }
    }

    @Test
    public void testConcurrentDuplicateDetection() throws Exception {
        var threadCount = 8;
        var messageCount = 5_000;
        var bin = new AtomicBitArrayBin(messageCount + 1000);

        // Pre-populate
        for (var i = 0; i < messageCount; i++) {
            bin.setBit(i, true);
        }

        var executor = Executors.newFixedThreadPool(threadCount);
        var barrier = new CyclicBarrier(threadCount);
        var missedDuplicates = new AtomicInteger(0);

        var futures = new ArrayList<Future<?>>();
        for (var t = 0; t < threadCount; t++) {
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (var i = 0; i < messageCount; i++) {
                    var wasDuplicate = bin.setBit(i, true);
                    if (!wasDuplicate) {
                        missedDuplicates.incrementAndGet();
                    }
                }
            }));
        }

        for (var f : futures) f.get();
        executor.shutdown();

        assertEquals("all re-sets should detect duplicate", 0, missedDuplicates.get());
    }

    @Test
    public void testConcurrentSharedIndices() throws Exception {
        var threadCount = 8;
        var messageCount = 10_000;
        var bin = new AtomicBitArrayBin(messageCount + 1000);

        var executor = Executors.newFixedThreadPool(threadCount);
        var barrier = new CyclicBarrier(threadCount);

        // All threads write the SAME indices. Exactly one thread should see
        // "not duplicate" per index; all others should see "duplicate".
        var firstSetBy = ConcurrentHashMap.newKeySet();

        var futures = new ArrayList<Future<?>>();
        for (var t = 0; t < threadCount; t++) {
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (var i = 0; i < messageCount; i++) {
                    var wasDuplicate = bin.setBit(i, true);
                    if (!wasDuplicate) {
                        var added = firstSetBy.add(i);
                        assertTrue("only one thread should be first to set " + i, added);
                    }
                }
            }));
        }

        for (var f : futures) f.get();
        executor.shutdown();

        assertEquals("every index should have exactly one first-setter",
                messageCount, firstSetBy.size());
    }

    @Test
    public void testConcurrentWindowAdvancement() throws Exception {
        var threadCount = 4;
        var windowSize = 256;
        var bin = new AtomicBitArrayBin(windowSize);

        var executor = Executors.newFixedThreadPool(threadCount);
        var barrier = new CyclicBarrier(threadCount);

        // Threads write to different ranges, some forcing window advancement
        var futures = new ArrayList<Future<?>>();
        for (var t = 0; t < threadCount; t++) {
            final var threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                // Each thread writes a range that may overlap and force advancement
                var base = (long) threadId * 200;
                for (var i = base; i < base + 500; i++) {
                    bin.setBit(i, true);
                }
            }));
        }

        for (var f : futures) f.get();
        executor.shutdown();

        // Verify the latest values are correct (earlier ones may have been evicted)
        var highestBase = (long)(threadCount - 1) * 200;
        for (var i = highestBase; i < highestBase + 500; i++) {
            assertTrue("bit " + i + " should be set or behind window", bin.getBit(i));
        }
    }

    @Test
    public void testSlotReuse() {
        // Small window to force rapid slot reuse
        var bin = new AtomicBitArrayBin(64);

        // First window: set some bits
        for (var i = 0; i < 64; i++) {
            bin.setBit(i, true);
        }

        // Advance past the first window
        bin.setBit(200, true);

        // The new bit should be set
        assertTrue(bin.getBit(200));

        // Bits in the new range that weren't explicitly set should be clear
        assertFalse(bin.getBit(201));
    }

    @Test
    public void testRollback() {
        var bin = new AtomicBitArrayBin(512);

        bin.setBit(42, true);
        assertTrue(bin.getBit(42));

        // Rollback (clear)
        bin.setBit(42, false);
        assertFalse(bin.getBit(42));

        // Should be able to re-set after rollback
        assertFalse("re-set after rollback should return false", bin.setBit(42, true));
        assertTrue(bin.getBit(42));
    }

    @Test
    public void testCapacityCalculation() {
        // windowSize 2048 → capacity = ((2048+1)/64)+1 = 33
        var bin = new AtomicBitArrayBin(2048);
        assertEquals(33, bin.getCapacity());

        // windowSize 64 → capacity = ((64+1)/64)+1 = 2
        var bin2 = new AtomicBitArrayBin(64);
        assertEquals(2, bin2.getCapacity());

        // windowSize 1 → capacity = max(1, ((1+1)/64)+1) = 1
        var bin3 = new AtomicBitArrayBin(1);
        assertEquals(1, bin3.getCapacity());
    }

    @Test
    public void testBehindWindowSetBitMatchesBitArrayBin() {
        var windowSize = 128;
        var original = new BitArrayBin(windowSize);
        var atomic = new AtomicBitArrayBin(windowSize);

        assertEquals(original.setBit(0, true), atomic.setBit(0, true));
        // Advance both windows far past index 0 (both land on origin 320)
        assertEquals(original.setBit(500, true), atomic.setBit(500, true));

        // Behind-window semantics must match BitArrayBin: setBit is a no-op
        // returning false. Reporting "duplicate" here would drop a legitimate
        // late message.
        assertEquals("behind-window setBit(true) parity",
                original.setBit(0, true), atomic.setBit(0, true));
        assertEquals("behind-window setBit(false) parity",
                original.setBit(0, false), atomic.setBit(0, false));
        assertEquals("behind-window getBit parity",
                original.getBit(0), atomic.getBit(0));
    }

    @Test
    public void testRollbackThenResendAfterWindowJump() {
        var bin = new AtomicBitArrayBin(128);

        assertFalse("first delivery is not a duplicate", bin.setBit(100, true));

        // An unrelated large sequence advances the window past index 100
        bin.setBit(10_000, true);

        // Rollback of the failed delivery: behind window, no-op
        bin.setBit(100, false);

        // The resend must NOT be reported as a duplicate — that would make
        // the rolled-back message undeliverable forever
        assertFalse("resend after rollback must not be dropped as duplicate",
                bin.setBit(100, true));
    }

    @Test
    public void testIsInOrderNegativeIndexDoesNotPoison() {
        var bin = new AtomicBitArrayBin(512);

        assertTrue(bin.isInOrder(0));
        assertTrue(bin.isInOrder(1));

        // -1 is what IdGenerator.getSequenceFromId returns for unparseable ids.
        // It must not be reported in-order and must not reset order tracking
        // to the initial "anything is in order" state.
        assertFalse("negative index is never in order", bin.isInOrder(-1));
        assertFalse("gap after invalid index must not be in order", bin.isInOrder(50));
        assertTrue("sequence resumes after the gap", bin.isInOrder(51));
    }

    /**
     * Cross-epoch ABA plant detector.
     *
     * <p>capacity=2 ring: epochs f and f-2 share a slot. Writers hammer the
     * trailing in-window epoch (frontier-1) at offsets 0..15 while the jumper
     * advances the window one epoch per round. A writer that is mid-setBit
     * when its slot is recycled must NOT leave a bit in the new epoch.
     *
     * <p>Writers never target the leading epoch f, so any bit in offsets 0..15
     * of a freshly recycled leading epoch is a planted bit — a future message
     * at that index would be falsely reported duplicate and dropped.
     */
    @Test(timeout = 60_000)
    public void testNoCrossEpochBitPlantDuringRecycle() throws Exception {
        var bin = new AtomicBitArrayBin(64);
        assertEquals("test requires a 2-slot ring", 2, bin.getCapacity());

        var rounds = 50_000;
        var writerThreads = 2;
        var frontier = new AtomicLong(1);
        var stop = new AtomicBoolean(false);
        var executor = Executors.newFixedThreadPool(writerThreads);

        var writers = new ArrayList<Future<?>>();
        for (var t = 0; t < writerThreads; t++) {
            writers.add(executor.submit(() -> {
                while (!stop.get()) {
                    var trailing = frontier.get() - 1;
                    var base = trailing * 64;
                    for (var o = 0; o < 16; o++) {
                        bin.setBit(base + o, true);
                    }
                }
            }));
        }

        var plantedAt = -1L;
        try {
            for (var f = 2L; f <= rounds; f++) {
                // Recycles the slot previously holding epoch f-2 while
                // writers may be mid-flight on trailing epochs
                bin.setBit(f * 64 + 63, true);

                // Probe the freshly recycled epoch before writers can
                // legitimately reach it (they only ever target frontier-1 < f)
                for (var scan = 0; scan < 3 && plantedAt < 0; scan++) {
                    for (var o = 0; o < 16; o++) {
                        if (bin.getBit(f * 64 + o)) {
                            plantedAt = f * 64 + o;
                            break;
                        }
                    }
                    Thread.onSpinWait();
                }
                if (plantedAt >= 0) {
                    break;
                }
                frontier.set(f);
            }
        } finally {
            stop.set(true);
            for (var w : writers) w.get();
            executor.shutdown();
        }

        assertEquals("bit planted into recycled epoch (cross-epoch ABA) at index "
                + plantedAt, -1L, plantedAt);
    }

    /**
     * Fabricated-index detector for getLastSetIndex.
     *
     * <p>Every written index is logged before the write. A concurrent scanner
     * asserts every getLastSetIndex() result was actually written. A torn
     * epoch/bits read across a slot recycle reconstructs an index (old epoch
     * &times; new bits) that exists in neither.
     */
    @Test(timeout = 60_000)
    public void testGetLastSetIndexNeverFabricatesIndex() throws Exception {
        var bin = new AtomicBitArrayBin(64);
        assertEquals("test requires a 2-slot ring", 2, bin.getCapacity());

        var rounds = 50_000;
        var frontier = new AtomicLong(1);
        var stop = new AtomicBoolean(false);
        var writeLog = ConcurrentHashMap.<Long>newKeySet();
        var executor = Executors.newFixedThreadPool(2);

        var fabricated = new AtomicLong(-1);

        // Writer: hammers the trailing epoch, offset encodes the epoch (e % 16)
        var writer = executor.submit(() -> {
            while (!stop.get()) {
                var trailing = frontier.get() - 1;
                var index = trailing * 64 + (trailing % 16);
                writeLog.add(index);
                bin.setBit(index, true);
            }
        });

        // Scanner: every result of getLastSetIndex must have been written
        var scanner = executor.submit(() -> {
            while (!stop.get()) {
                var last = bin.getLastSetIndex();
                if (last >= 0 && !writeLog.contains(last)) {
                    fabricated.compareAndSet(-1, last);
                    return;
                }
            }
        });

        try {
            // Jumper: offset also encodes the epoch (32 + f % 16), so an old
            // epoch paired with new bits yields an unlogged index
            for (var f = 2L; f <= rounds && fabricated.get() < 0; f++) {
                var index = f * 64 + 32 + (f % 16);
                writeLog.add(index);
                bin.setBit(index, true);
                frontier.set(f);
            }
        } finally {
            stop.set(true);
            writer.get();
            scanner.get();
            executor.shutdown();
        }

        assertEquals("getLastSetIndex returned an index that was never written: "
                + fabricated.get(), -1L, fabricated.get());
    }

    @Test(timeout = 30_000)
    public void testCapacityOneConcurrentChurn() throws Exception {
        // Degenerate single-slot ring: every 64-index step recycles the slot
        var bin = new AtomicBitArrayBin(1);
        assertEquals(1, bin.getCapacity());

        var threadCount = 2;
        var perThread = 20_000;
        var executor = Executors.newFixedThreadPool(threadCount);
        var barrier = new CyclicBarrier(threadCount);

        var futures = new ArrayList<Future<?>>();
        for (var t = 0; t < threadCount; t++) {
            final var threadId = t;
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (var i = 0; i < perThread; i++) {
                    bin.setBit((long) i * threadCount + threadId, true);
                }
            }));
        }

        for (var f : futures) f.get();
        executor.shutdown();

        // Terminates without hang (timeout) and the ring is still coherent
        var last = bin.getLastSetIndex();
        assertTrue("last set index should be near the top of the range, was " + last,
                last >= (long) (perThread - 64) * threadCount - 64);
    }

    @Test
    public void testStressSequentialThenConcurrentVerify() throws Exception {
        var windowSize = 4096;
        var messageCount = 3000;
        var bin = new AtomicBitArrayBin(windowSize);

        // Sequential population
        for (var i = 0; i < messageCount; i++) {
            assertFalse(bin.setBit(i, true));
        }

        // Concurrent verification
        var threadCount = 8;
        var executor = Executors.newFixedThreadPool(threadCount);
        var barrier = new CyclicBarrier(threadCount);
        var failures = new AtomicInteger(0);

        var futures = new ArrayList<Future<?>>();
        for (var t = 0; t < threadCount; t++) {
            futures.add(executor.submit(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (var i = 0; i < messageCount; i++) {
                    if (!bin.getBit(i)) {
                        failures.incrementAndGet();
                    }
                }
            }));
        }

        for (var f : futures) f.get();
        executor.shutdown();

        assertEquals("concurrent reads should see all set bits", 0, failures.get());
    }
}
