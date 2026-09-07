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
import java.util.concurrent.atomic.AtomicReferenceArray;

/**
 * A lock-free replacement for {@link BitArrayBin} that uses a ring buffer of
 * immutable-epoch slot holders with CAS-based bit operations.
 *
 * <p>The upstream {@code BitArrayBin} stores bits in a {@code LinkedList<BitArray>}
 * and requires external synchronization for all access. This class replaces that
 * with a fixed-size ring of {@link Slot} objects where each slot holds 64 bits.
 * All operations use CAS (compare-and-swap) instead of locks.
 *
 * <h3>Ring buffer design</h3>
 *
 * <p>Slots are addressed by absolute position: {@code ringPos = (index / 64) % capacity}.
 * Each slot object carries a fixed epoch identifying which absolute 64-bit block
 * it holds. The window covers exactly {@code capacity} consecutive epochs
 * {@code [originEpoch, originEpoch + capacity)}, so two distinct in-window epochs
 * never share a ring position.
 *
 * <p>When the window advances, an evicted slot is recycled by CAS-replacing the
 * whole slot object with a fresh one for the new epoch. Because the epoch is
 * immutable and the bits live inside the slot object, a writer that captured the
 * old slot can only ever mutate the orphaned object — it can never corrupt the
 * new epoch's bits. Every mutation is therefore linearizable: it either lands in
 * the live slot, or in an orphan, which is equivalent to the operation having
 * completed just before the window advanced.
 *
 * <h3>Concurrency guarantees</h3>
 * <ul>
 *   <li><b>Common path</b> (bit set/get within current window): lock-free,
 *       single CAS on the slot's bits</li>
 *   <li><b>Window advance</b> (rare, only on sequence jumps): one CAS on the
 *       origin plus one reference CAS per recycled slot; there is no
 *       multi-step reclamation state, so a stalled thread can never block
 *       other threads' progress</li>
 *   <li><b>No global lock</b>: threads operating on different bit indices within
 *       the same bin only contend if they hash to the same 64-bit slot</li>
 * </ul>
 *
 * <h3>Behind-window semantics</h3>
 *
 * <p>Matching {@link BitArrayBin}: {@code setBit} for an index behind the window
 * is a no-op returning {@code false} (the message is accepted rather than
 * reported as a duplicate, favoring at-least-once delivery over message loss),
 * while {@code getBit} returns {@code true} (the range is assumed seen).
 */
public class AtomicBitArrayBin {

    static final int LONG_SIZE = 64;

    private static final long UNINITIALIZED = -1L;

    private final int capacity;
    private final AtomicReferenceArray<Slot> slots;
    private final AtomicLong origin;
    private final AtomicLong lastInOrderBit;

    /**
     * One 64-bit block of the ring. The epoch is fixed at construction; only
     * the bits mutate. Recycling replaces the whole object with a single
     * reference CAS, so the epoch/bits pair a reader observes is always
     * internally consistent.
     */
    private static final class Slot {
        final long epoch;
        final AtomicLong bits = new AtomicLong();

        Slot(long epoch) {
            this.epoch = epoch;
        }
    }

    public AtomicBitArrayBin(int windowSize) {
        capacity = Math.max(1, ((windowSize + 1) / LONG_SIZE) + 1);
        slots = new AtomicReferenceArray<>(capacity);
        origin = new AtomicLong(0);
        lastInOrderBit = new AtomicLong(UNINITIALIZED);
    }

    /**
     * Set or clear a bit at the given index.
     *
     * @param index the absolute bit index (message sequence number)
     * @param value true to set, false to clear
     * @return the previous value of the bit (true if it was already set);
     *         always false for an index behind the window (no-op)
     */
    public boolean setBit(long index, boolean value) {
        if (index < 0) return false;

        while (true) {
            var orig = origin.get();
            var epoch = index / LONG_SIZE;
            var originEpoch = orig / LONG_SIZE;

            if (epoch < originEpoch) {
                return false;
            }

            if (epoch >= originEpoch + capacity) {
                advanceOrigin(orig, epoch);
                continue;
            }

            var slot = slotFor(epoch);
            if (slot == null) {
                // Recycled to a newer epoch, which proves this epoch is now
                // behind the window: re-read the origin and reclassify
                continue;
            }

            var mask = 1L << (int)(index % LONG_SIZE);
            while (true) {
                var oldBits = slot.bits.get();
                var wasSet = (oldBits & mask) != 0;

                if (value) {
                    if (wasSet) return true;
                    if (slot.bits.compareAndSet(oldBits, oldBits | mask)) return false;
                } else {
                    if (!wasSet) return false;
                    if (slot.bits.compareAndSet(oldBits, oldBits & ~mask)) return true;
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

        var slot = slots.get((int)(epoch % capacity));
        if (slot == null) {
            return false;
        }
        if (slot.epoch != epoch) {
            // Newer epoch means this one was evicted (behind-window semantics);
            // an older epoch means this block was never written
            return slot.epoch > epoch;
        }
        return (slot.bits.get() & (1L << (int)(index % LONG_SIZE))) != 0;
    }

    /**
     * Test if the index is the next expected in-order sequence.
     *
     * @param index the absolute bit index
     * @return true if this is the next in-order message; always false for a
     *         negative index (which also leaves order tracking untouched)
     */
    public boolean isInOrder(long index) {
        if (index < 0) {
            return false;
        }
        var prev = lastInOrderBit.getAndSet(index);
        return prev == UNINITIALIZED || prev + 1 == index;
    }

    /**
     * Get the index of the highest set bit across all valid slots.
     *
     * <p>Snapshot semantics: concurrent writers may set higher bits while the
     * scan runs; the result reflects some recent consistent state.
     *
     * @return the highest set bit index, or -1 if no bits are set
     */
    public long getLastSetIndex() {
        var originEpoch = origin.get() / LONG_SIZE;

        for (var offset = capacity - 1; offset >= 0; offset--) {
            var epoch = originEpoch + offset;
            var slot = slots.get((int)(epoch % capacity));
            if (slot == null || slot.epoch != epoch) {
                continue;
            }
            var slotBits = slot.bits.get();
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

    /**
     * Resolve the live slot for an epoch, installing a fresh one if the ring
     * position holds an older epoch (or none). Installed epochs per position
     * are strictly increasing, so the loop terminates: every CAS failure means
     * another thread installed a newer slot.
     *
     * @return the slot owning this epoch, or null if the position was already
     *         recycled to a newer epoch (proving this epoch is behind the window)
     */
    private Slot slotFor(long epoch) {
        var ringPos = (int)(epoch % capacity);
        while (true) {
            var slot = slots.get(ringPos);
            if (slot != null) {
                if (slot.epoch == epoch) {
                    return slot;
                }
                if (slot.epoch > epoch) {
                    return null;
                }
            }
            var fresh = new Slot(epoch);
            if (slots.compareAndSet(ringPos, slot, fresh)) {
                return fresh;
            }
        }
    }
}
