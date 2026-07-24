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

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * A lock-free replacement for {@link BitArrayBin} that uses {@link AtomicLongArray}
 * as a ring buffer with CAS-based bit operations.
 *
 * <p>The upstream {@code BitArrayBin} stores bits in a {@code LinkedList<BitArray>}
 * and requires external synchronization for all access. This class replaces that
 * with a fixed-size {@code AtomicLongArray} ring buffer where each slot holds 64 bits.
 * All operations use CAS (compare-and-swap) instead of locks.
 *
 * <h3>Ring buffer design</h3>
 *
 * <p>Slots are addressed by absolute position: {@code ringPos = (index / 64) % capacity}.
 * This mapping is independent of the window origin, so when the window advances,
 * slots that remain in the window keep their data without copying. Only evicted
 * slots need to be reclaimed, which happens lazily on first access.
 *
 * <p>Each slot carries an epoch ({@code slotEpoch[ringPos]}) identifying which
 * absolute 64-bit block it holds. On access, if the slot's epoch doesn't match
 * the expected epoch, the slot is reclaimed via a brief CAS protocol: the epoch
 * is set to a {@code CLEARING} sentinel, bits are zeroed, and the epoch is
 * published to the new value. Concurrent readers that see {@code CLEARING}
 * retry until the transition completes.
 *
 * <h3>Concurrency guarantees</h3>
 * <ul>
 *   <li><b>Common path</b> (bit set/get within current window): fully lock-free,
 *       single CAS on the bits slot</li>
 *   <li><b>Window advance</b> (rare, only on sequence jumps): single CAS on
 *       the origin; slot reclamation uses a brief per-slot CAS protocol</li>
 *   <li><b>No global lock</b>: threads operating on different bit indices within
 *       the same bin only contend if they hash to the same 64-bit slot</li>
 * </ul>
 */
public class AtomicBitArrayBin {

    static final int LONG_SIZE = 64;

    private static final long UNINITIALIZED = -1L;
    private static final long CLEARING = Long.MAX_VALUE;

    private final int capacity;
    private final AtomicLongArray bits;
    private final AtomicLongArray slotEpoch;
    private final AtomicLong origin;
    private final AtomicLong lastInOrderBit;

    public AtomicBitArrayBin(int windowSize) {
        capacity = Math.max(1, ((windowSize + 1) / LONG_SIZE) + 1);
        bits = new AtomicLongArray(capacity);
        slotEpoch = new AtomicLongArray(capacity);
        for (var i = 0; i < capacity; i++) {
            slotEpoch.set(i, UNINITIALIZED);
        }
        origin = new AtomicLong(0);
        lastInOrderBit = new AtomicLong(UNINITIALIZED);
    }

    /**
     * Set or clear a bit at the given index.
     *
     * @param index the absolute bit index (message sequence number)
     * @param value true to set, false to clear
     * @return the previous value of the bit (true if it was already set)
     */
    public boolean setBit(long index, boolean value) {
        if (index < 0) return false;

        while (true) {
            var orig = origin.get();
            var epoch = index / LONG_SIZE;
            var originEpoch = orig / LONG_SIZE;

            if (epoch < originEpoch) {
                return true;
            }

            if (epoch >= originEpoch + capacity) {
                advanceOrigin(orig, epoch);
                continue;
            }

            var ringPos = (int)(epoch % capacity);
            var bitOffset = (int)(index % LONG_SIZE);
            long mask = 1L << bitOffset;

            if (!ensureSlotEpoch(ringPos, epoch)) {
                continue;
            }

            while (true) {
                if (slotEpoch.get(ringPos) != epoch) {
                    break;
                }

                var oldBits = bits.get(ringPos);
                var wasSet = (oldBits & mask) != 0;

                if (value) {
                    if (wasSet) return true;
                    if (bits.compareAndSet(ringPos, oldBits, oldBits | mask)) return false;
                } else {
                    if (!wasSet) return false;
                    if (bits.compareAndSet(ringPos, oldBits, oldBits & ~mask)) return true;
                }
            }
        }
    }

    /**
     * Get the boolean value at the index.
     *
     * @param index the absolute bit index
     * @return true if the bit is set, or if the index is behind the window
     */
    public boolean getBit(long index) {
        if (index < 0) return false;

        var orig = origin.get();
        var epoch = index / LONG_SIZE;
        var originEpoch = orig / LONG_SIZE;

        if (epoch < originEpoch) return true;
        if (epoch >= originEpoch + capacity) return false;

        var ringPos = (int)(epoch % capacity);
        var curEpoch = slotEpoch.get(ringPos);

        if (curEpoch != epoch) {
            return curEpoch > epoch && curEpoch != UNINITIALIZED;
        }

        var bitOffset = (int)(index % LONG_SIZE);
        return (bits.get(ringPos) & (1L << bitOffset)) != 0;
    }

    /**
     * Test if the index is the next expected in-order sequence.
     *
     * @param index the absolute bit index
     * @return true if this is the next in-order message
     */
    public boolean isInOrder(long index) {
        var prev = lastInOrderBit.getAndSet(index);
        return prev == UNINITIALIZED || prev + 1 == index;
    }

    /**
     * Get the index of the highest set bit across all valid slots.
     *
     * @return the highest set bit index, or -1 if no bits are set
     */
    public long getLastSetIndex() {
        var orig = origin.get();
        var originEpoch = orig / LONG_SIZE;

        for (int offset = capacity - 1; offset >= 0; offset--) {
            var epoch = originEpoch + offset;
            var ringPos = (int)(epoch % capacity);

            var curEpoch = slotEpoch.get(ringPos);
            if (curEpoch != epoch || curEpoch == CLEARING) continue;

            var slotBits = bits.get(ringPos);
            if (slotBits != 0) {
                var highBit = LONG_SIZE - 1 - Long.numberOfLeadingZeros(slotBits);
                return epoch * LONG_SIZE + highBit;
            }
        }
        return -1;
    }

    /**
     * @return the number of 64-bit slots in the ring buffer
     */
    public int getCapacity() {
        return capacity;
    }

    private void advanceOrigin(long currentOrigin, long targetEpoch) {
        var newOriginEpoch = targetEpoch - capacity + 1;
        var newOrigin = Math.max(0, newOriginEpoch * LONG_SIZE);
        if (newOrigin > currentOrigin) {
            origin.compareAndSet(currentOrigin, newOrigin);
        }
    }

    private boolean ensureSlotEpoch(int ringPos, long expectedEpoch) {
        var curEpoch = slotEpoch.get(ringPos);

        if (curEpoch == expectedEpoch) return true;

        if (curEpoch == CLEARING) {
            Thread.yield();
            return false;
        }

        if (curEpoch > expectedEpoch && curEpoch != UNINITIALIZED) {
            return false;
        }

        if (slotEpoch.compareAndSet(ringPos, curEpoch, CLEARING)) {
            bits.set(ringPos, 0);
            slotEpoch.set(ringPos, expectedEpoch);
            return true;
        }

        return false;
    }
}
