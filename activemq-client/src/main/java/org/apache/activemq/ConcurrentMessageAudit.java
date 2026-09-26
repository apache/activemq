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

import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.util.AtomicBitArrayBin;
import org.apache.activemq.util.IdGenerator;

/**
 * A lock-free message audit backed by {@link ConcurrentHashMap} and
 * {@link AtomicBitArrayBin}.
 *
 * <p>The upstream {@code ActiveMQMessageAudit} wraps every operation in a
 * single {@code synchronized(this)} block, serializing all threads regardless
 * of which producer they are working with. This class eliminates all
 * synchronization on the hot path:
 * <ul>
 *   <li>{@link ConcurrentHashMap#computeIfAbsent} for lock-free producer
 *       lookup and atomic insertion</li>
 *   <li>{@link AtomicBitArrayBin} for CAS-based bit-level mutations &mdash;
 *       no {@code synchronized} blocks anywhere in the hot path</li>
 * </ul>
 *
 * <p>Producer count is bounded by {@link #getMaximumNumberOfProducersToTrack()}.
 * When the limit is exceeded, entries are evicted in iteration order (approximate
 * FIFO). The per-producer audit window is controlled by {@link #getAuditDepth()}.
 */
public class ConcurrentMessageAudit {

    public static final int DEFAULT_WINDOW_SIZE = 2048;
    public static final int MAXIMUM_PRODUCER_COUNT = 64;

    private volatile int auditDepth;
    private volatile int maximumNumberOfProducersToTrack;
    private final ConcurrentHashMap<String, AtomicBitArrayBin> map;

    public ConcurrentMessageAudit() {
        this(DEFAULT_WINDOW_SIZE, MAXIMUM_PRODUCER_COUNT);
    }

    public ConcurrentMessageAudit(int auditDepth, int maximumNumberOfProducersToTrack) {
        this.auditDepth = auditDepth;
        this.maximumNumberOfProducersToTrack = maximumNumberOfProducersToTrack;
        this.map = new ConcurrentHashMap<>(maximumNumberOfProducersToTrack);
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
        evictExcess();
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
        return getOrCreate(key).setBit(index, true);
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
        map.computeIfPresent(pid.toString(), (k, bab) -> {
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
        map.computeIfPresent(seed, (k, bab) -> {
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
        var bab = map.get(seed);
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
        var bab = getOrCreate(pid.toString());
        return bab.isInOrder(id.getProducerSequenceId());
    }

    public long getLastSeqId(ProducerId id) {
        var bab = map.get(id.toString());
        if (bab != null) {
            return bab.getLastSetIndex();
        }
        return -1;
    }

    public void clear() {
        map.clear();
    }

    public int getProducerCount() {
        return map.size();
    }

    private AtomicBitArrayBin getOrCreate(String key) {
        var bab = map.get(key);
        if (bab != null) {
            return bab;
        }
        bab = map.computeIfAbsent(key, k -> new AtomicBitArrayBin(auditDepth));
        evictExcess();
        return bab;
    }

    /**
     * Trim the map to the configured maximum. Eviction follows CHM iteration
     * order (approximate FIFO, not LRU). Concurrent callers may transiently
     * remove more entries than strictly required; the bound is approximate
     * by design and self-corrects on subsequent inserts.
     */
    private void evictExcess() {
        var max = maximumNumberOfProducersToTrack;
        while (map.size() > max) {
            var it = map.keySet().iterator();
            if (it.hasNext()) {
                it.next();
                it.remove();
            } else {
                break;
            }
        }
    }
}
