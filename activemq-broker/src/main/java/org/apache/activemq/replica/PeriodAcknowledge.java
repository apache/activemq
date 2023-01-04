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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQSession;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class PeriodAcknowledge {

    private static final int MAX_ACK_BATCH_SIZE = 100;
    private boolean safeToAck = true;
    private final AtomicLong lastAckTime = new AtomicLong();
    private final AtomicInteger pendingAckCount = new AtomicInteger();
    private final AtomicReference<ActiveMQConnection> connection = new AtomicReference<>();
    private final AtomicReference<ActiveMQSession> connectionSession = new AtomicReference<>();
    private final long replicaAckPeriod;
    private final Object periodicCommitLock = new Object();


    public PeriodAcknowledge(long replicaAckPeriod) {
        this.replicaAckPeriod = replicaAckPeriod;
    }

    public void setConnection(ActiveMQConnection activeMQConnection) {
        connection.set(activeMQConnection);
    }

    public void setConnectionSession(ActiveMQSession activeMQSession) {
        connectionSession.set(activeMQSession);
    }

    public void setSafeToAck(boolean safeToAck) {
        this.safeToAck = safeToAck;
    }

    private boolean shouldPeriodicallyCommit() {
        return System.currentTimeMillis() - lastAckTime.get() >= replicaAckPeriod;
    }

    private boolean reachedMaxAckBatchSize() {
        return pendingAckCount.incrementAndGet() >= MAX_ACK_BATCH_SIZE;
    }

    public void acknowledge() throws Exception {
        if (connection.get() == null || connectionSession.get() == null || !safeToAck) {
            return;
        }

        synchronized (periodicCommitLock) {
            if (reachedMaxAckBatchSize() || shouldPeriodicallyCommit()) {
                connectionSession.get().acknowledge();
                lastAckTime.set(System.currentTimeMillis());
                pendingAckCount.set(0);
            }
        }
    }
}