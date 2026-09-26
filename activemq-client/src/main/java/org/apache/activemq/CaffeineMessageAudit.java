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

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.util.AtomicBitArrayBin;
import org.apache.activemq.util.IdGenerator;

/**
 * A message audit backed by Caffeine cache and {@link AtomicBitArrayBin}.
 *
 * <p>Uses Caffeine's {@link Cache} with size-based eviction (TinyLfu policy)
 * for bounded producer tracking, paired with lock-free
 * {@link AtomicBitArrayBin} for per-producer bit operations.
 *
 * <p>Producer count is bounded by {@link #getMaximumNumberOfProducersToTrack()}.
 * Caffeine's TinyLfu admission policy evicts entries based on frequency and
 * recency, providing near-optimal hit rates. The per-producer audit window is
 * controlled by {@link #getAuditDepth()}.
 *
 * <p>All per-producer bit operations are lock-free via {@link AtomicBitArrayBin}.
 * The only synchronization points are within Caffeine's internal structures
 * for cache management.
 */
public class CaffeineMessageAudit {

    public static final int DEFAULT_WINDOW_SIZE = 2048;
    public static final int MAXIMUM_PRODUCER_COUNT = 64;

    private volatile int auditDepth;
    private volatile int maximumNumberOfProducersToTrack;
    private final Cache<String, AtomicBitArrayBin> cache;

    public CaffeineMessageAudit() {
        this(DEFAULT_WINDOW_SIZE, MAXIMUM_PRODUCER_COUNT);
    }

    public CaffeineMessageAudit(int auditDepth, int maximumNumberOfProducersToTrack) {
        this.auditDepth = auditDepth;
        this.maximumNumberOfProducersToTrack = maximumNumberOfProducersToTrack;
        this.cache = buildCache(maximumNumberOfProducersToTrack);
    }

    public int getAuditDepth() {
        return auditDepth;
    }

    public void setAuditDepth(int auditDepth) {
        this.auditDepth = auditDepth;
    }

    public int getMaximumNumberOfProducersToTrack() {
        return maximumNumberOfProducersToTrack;
    }

    public void setMaximumNumberOfProducersToTrack(int maximumNumberOfProducersToTrack) {
        this.maximumNumberOfProducersToTrack = maximumNumberOfProducersToTrack;
        // Resize in place: rebuilding and swapping the cache would lose
        // entries inserted concurrently with the copy
        cache.policy().eviction()
                .ifPresent(eviction -> eviction.setMaximum(maximumNumberOfProducersToTrack));
    }

    public boolean isDuplicate(String id) {
        var seed = IdGenerator.getSeedFromId(id);
        if (seed == null) {
            return false;
        }
        var index = IdGenerator.getSequenceFromId(id);
        if (index < 0) {
            return false;
        }
        return markSeen(seed, index);
    }

    public boolean isDuplicate(final MessageId id) {
        if (id == null) {
            return false;
        }
        var pid = id.getProducerId();
        if (pid == null) {
            return false;
        }
        return markSeen(pid.toString(), id.getProducerSequenceId());
    }

    /**
     * Record the index against the bin for the key. If the bin is evicted
     * between lookup and the bit write, the write lands in an orphaned bin
     * and the next occurrence of the id is not detected as a duplicate.
     * This residual race is accepted: eviction only fires under producer
     * oversubscription, membership re-check-and-retry in that regime was
     * measured at a 2-5x throughput cost while the recreated entry is
     * immediately evicted again, and the failure direction (duplicate
     * redelivery) is tolerable under at-least-once delivery.
     */
    private boolean markSeen(String key, long index) {
        return cache.get(key, k -> new AtomicBitArrayBin(auditDepth)).setBit(index, true);
    }

    public void rollback(final MessageId id) {
        if (id == null) {
            return;
        }
        var pid = id.getProducerId();
        if (pid == null) {
            return;
        }
        var seqId = id.getProducerSequenceId();
        cache.asMap().computeIfPresent(pid.toString(), (k, bab) -> {
            bab.setBit(seqId, false);
            return bab;
        });
    }

    public void rollback(final String id) {
        var seed = IdGenerator.getSeedFromId(id);
        if (seed == null) {
            return;
        }
        var index = IdGenerator.getSequenceFromId(id);
        if (index < 0) {
            return;
        }
        cache.asMap().computeIfPresent(seed, (k, bab) -> {
            bab.setBit(index, false);
            return bab;
        });
    }

    public boolean isInOrder(final String id) {
        if (id == null) {
            return true;
        }
        var seed = IdGenerator.getSeedFromId(id);
        if (seed == null) {
            return true;
        }
        var bab = cache.getIfPresent(seed);
        if (bab != null) {
            var index = IdGenerator.getSequenceFromId(id);
            return bab.isInOrder(index);
        }
        return true;
    }

    public boolean isInOrder(final MessageId id) {
        if (id == null) {
            return false;
        }
        var pid = id.getProducerId();
        if (pid == null) {
            return false;
        }
        var bab = cache.get(pid.toString(), k -> new AtomicBitArrayBin(auditDepth));
        return bab.isInOrder(id.getProducerSequenceId());
    }

    public long getLastSeqId(ProducerId id) {
        var bab = cache.getIfPresent(id.toString());
        if (bab != null) {
            return bab.getLastSetIndex();
        }
        return -1;
    }

    public void clear() {
        cache.invalidateAll();
    }

    public int getProducerCount() {
        cache.cleanUp();
        return (int) cache.estimatedSize();
    }

    private static Cache<String, AtomicBitArrayBin> buildCache(int maxSize) {
        return Caffeine.newBuilder()
                .maximumSize(maxSize)
                .build();
    }
}
