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
    private volatile Cache<String, AtomicBitArrayBin> cache;

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
        var newCache = buildCache(maximumNumberOfProducersToTrack);
        newCache.putAll(this.cache.asMap());
        this.cache = newCache;
    }

    public boolean isDuplicate(String id) {
        var seed = IdGenerator.getSeedFromId(id);
        if (seed == null) {
            return false;
        }
        var bab = cache.get(seed, k -> new AtomicBitArrayBin(auditDepth));
        var index = IdGenerator.getSequenceFromId(id);
        if (index >= 0) {
            return bab.setBit(index, true);
        }
        return false;
    }

    public boolean isDuplicate(final MessageId id) {
        if (id == null) {
            return false;
        }
        var pid = id.getProducerId();
        if (pid == null) {
            return false;
        }
        var bab = cache.get(pid.toString(), k -> new AtomicBitArrayBin(auditDepth));
        return bab.setBit(id.getProducerSequenceId(), true);
    }

    public void rollback(final MessageId id) {
        if (id == null) {
            return;
        }
        var pid = id.getProducerId();
        if (pid == null) {
            return;
        }
        var bab = cache.getIfPresent(pid.toString());
        if (bab != null) {
            bab.setBit(id.getProducerSequenceId(), false);
        }
    }

    public void rollback(final String id) {
        var seed = IdGenerator.getSeedFromId(id);
        if (seed == null) {
            return;
        }
        var bab = cache.getIfPresent(seed);
        if (bab != null) {
            var index = IdGenerator.getSequenceFromId(id);
            bab.setBit(index, false);
        }
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
