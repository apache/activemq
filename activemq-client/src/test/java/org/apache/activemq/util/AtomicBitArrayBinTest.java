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
import java.util.concurrent.atomic.AtomicInteger;

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
    public void testBehindWindowReturnsTrueOnSet() {
        var bin = new AtomicBitArrayBin(128);

        bin.setBit(0, true);
        // Advance window far past index 0
        bin.setBit(500, true);

        // Setting a behind-window bit should return true (treated as already set)
        assertTrue("behind-window setBit should return true", bin.setBit(0, true));
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
