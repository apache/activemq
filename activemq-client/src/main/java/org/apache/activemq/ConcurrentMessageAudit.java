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
        var bab = getOrCreate(seed);
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
        var bab = getOrCreate(pid.toString());
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
        var bab = map.get(pid.toString());
        if (bab != null) {
            bab.setBit(id.getProducerSequenceId(), false);
        }
    }

    public void rollback(final String id) {
        var seed = IdGenerator.getSeedFromId(id);
        if (seed == null) {
            return;
        }
        var bab = map.get(seed);
        if (bab != null) {
            long index = IdGenerator.getSequenceFromId(id);
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
