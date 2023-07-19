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
package org.apache.activemq.replica;

import org.apache.activemq.broker.jmx.MBeanInfo;

import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class ReplicaStatistics {

    private AtomicLong replicationTps;
    private AtomicLong tpsCounter;
    private AtomicLong lastTpsCounter;

    private AtomicLong totalReplicationLag;
    private AtomicLong sourceLastProcessedTime;
    private AtomicLong replicationLag;
    private AtomicLong replicaLastProcessedTime;

    private AtomicBoolean sourceReplicationFlowControl;
    private AtomicBoolean replicaReplicationFlowControl;

    public ReplicaStatistics() {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            if (tpsCounter == null) {
                return;
            }

            long c = tpsCounter.get();
            if (replicationTps == null) {
                replicationTps = new AtomicLong();
            }
            replicationTps.set((c - lastTpsCounter.get()) / 10);
            if (lastTpsCounter == null) {
                lastTpsCounter = new AtomicLong();
            }
            lastTpsCounter.set(c);
        }, 60, 60, TimeUnit.SECONDS);
    }

    public void reset() {
        replicationTps = null;
        tpsCounter = null;
        lastTpsCounter = null;

        totalReplicationLag = null;
        sourceLastProcessedTime = null;
        replicationLag = null;
        replicaLastProcessedTime = null;

        sourceReplicationFlowControl = null;
        replicaReplicationFlowControl = null;
    }

    public void increaseTpsCounter(long size) {
        if (tpsCounter == null) {
            tpsCounter = new AtomicLong();
        }
        tpsCounter.addAndGet(size);
    }

    public AtomicLong getReplicationTps() {
        return replicationTps;
    }

    public AtomicLong getTotalReplicationLag() {
        return totalReplicationLag;
    }

    public void setTotalReplicationLag(long totalReplicationLag) {
        if (this.totalReplicationLag == null) {
            this.totalReplicationLag = new AtomicLong();
        }
        this.totalReplicationLag.set(totalReplicationLag);
    }

    public AtomicLong getSourceLastProcessedTime() {
        return sourceLastProcessedTime;
    }

    public void setSourceLastProcessedTime(long sourceLastProcessedTime) {
        if (this.sourceLastProcessedTime == null) {
            this.sourceLastProcessedTime = new AtomicLong();
        }
        this.sourceLastProcessedTime.set(sourceLastProcessedTime);
    }

    public AtomicLong getReplicationLag() {
        return replicationLag;
    }

    public void setReplicationLag(long replicationLag) {
        if (this.replicationLag == null) {
            this.replicationLag = new AtomicLong();
        }
        this.replicationLag.set(replicationLag);
    }

    public AtomicLong getReplicaLastProcessedTime() {
        return replicaLastProcessedTime;
    }

    public void setReplicaLastProcessedTime(long replicaLastProcessedTime) {
        if (this.replicaLastProcessedTime == null) {
            this.replicaLastProcessedTime = new AtomicLong();
        }
        this.replicaLastProcessedTime.set(replicaLastProcessedTime);
    }

    public AtomicBoolean getSourceReplicationFlowControl() {
        return sourceReplicationFlowControl;
    }

    public void setSourceReplicationFlowControl(boolean sourceReplicationFlowControl) {
        if (this.sourceReplicationFlowControl == null) {
            this.sourceReplicationFlowControl = new AtomicBoolean();
        }
        this.sourceReplicationFlowControl.set(sourceReplicationFlowControl);
    }

    public AtomicBoolean getReplicaReplicationFlowControl() {
        return replicaReplicationFlowControl;
    }

    public void setReplicaReplicationFlowControl(boolean replicaReplicationFlowControl) {
        if (this.replicaReplicationFlowControl == null) {
            this.replicaReplicationFlowControl = new AtomicBoolean();
        }
        this.replicaReplicationFlowControl.set(replicaReplicationFlowControl);
    }
}
