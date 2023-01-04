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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

public class ReplicaReplicationQueueSupplier {

    private final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);
    private final CountDownLatch initializationLatch = new CountDownLatch(1);
    private final CountDownLatch sequenceInitializationLatch = new CountDownLatch(1);
    private ActiveMQQueue mainReplicationQueue = null; // memoized
    private ActiveMQQueue intermediateReplicationQueue = null; // memoized
    private ActiveMQQueue sequenceQueue = null; // memoized
    private final Broker broker;

    public ReplicaReplicationQueueSupplier(final Broker broker) {
        this.broker = requireNonNull(broker);
    }

    public ActiveMQQueue getMainQueue() {
        try {
            if (initializationLatch.await(1L, TimeUnit.MINUTES)) {
                return requireNonNull(mainReplicationQueue);
            }
        } catch (InterruptedException e) {
            throw new ActiveMQReplicaException("Interrupted while waiting for main replication queue initialization", e);
        }
        throw new ActiveMQReplicaException("Timed out waiting for main replication queue initialization");
    }

    public ActiveMQQueue getIntermediateQueue() {
        try {
            if (initializationLatch.await(1L, TimeUnit.MINUTES)) {
                return requireNonNull(intermediateReplicationQueue);
            }
        } catch (InterruptedException e) {
            throw new ActiveMQReplicaException("Interrupted while waiting for intermediate replication queue initialization", e);
        }
        throw new ActiveMQReplicaException("Timed out waiting for intermediate replication queue initialization");
    }

    public ActiveMQQueue getSequenceQueue() {
        try {
            if (sequenceInitializationLatch.await(1L, TimeUnit.MINUTES)) {
                return requireNonNull(sequenceQueue);
            }
        } catch (InterruptedException e) {
            throw new ActiveMQReplicaException("Interrupted while waiting for replication sequence queue initialization", e);
        }
        throw new ActiveMQReplicaException("Timed out waiting for replication sequence queue initialization");
    }

    public void initialize() {
        try {
            mainReplicationQueue = getOrCreateMainReplicationQueue();
            intermediateReplicationQueue = getOrCreateIntermediateReplicationQueue();
        } catch (Exception e) {
            logger.error("Could not obtain replication queues", e);
            throw new ActiveMQReplicaException("Failed to get or create replication queues");
        }
        initializationLatch.countDown();
    }

    public void initializeSequenceQueue() {
        try {
            sequenceQueue = getOrCreateSequenceQueue();
        } catch (Exception e) {
            logger.error("Could not obtain replication sequence queue", e);
            throw new ActiveMQReplicaException("Failed to get or create replication sequence queue");
        }
        sequenceInitializationLatch.countDown();

    }

    private ActiveMQQueue getOrCreateMainReplicationQueue() throws Exception {
        return getOrCreateQueue(ReplicaSupport.MAIN_REPLICATION_QUEUE_NAME);
    }

    private ActiveMQQueue getOrCreateIntermediateReplicationQueue() throws Exception {
        return getOrCreateQueue(ReplicaSupport.INTERMEDIATE_REPLICATION_QUEUE_NAME);
    }

    private ActiveMQQueue getOrCreateSequenceQueue() throws Exception {
        return getOrCreateQueue(ReplicaSupport.SEQUENCE_REPLICATION_QUEUE_NAME);
    }

    private ActiveMQQueue getOrCreateQueue(String replicationQueueName) throws Exception {
        Optional<ActiveMQDestination> existingReplicationQueue = broker.getDurableDestinations()
                .stream()
                .filter(ActiveMQDestination::isQueue)
                .filter(d -> replicationQueueName.equals(d.getPhysicalName()))
                .findFirst();
        if (existingReplicationQueue.isPresent()) {
            logger.debug("Existing replication queue {}", existingReplicationQueue.get().getPhysicalName());
            return new ActiveMQQueue(existingReplicationQueue.get().getPhysicalName());
        } else {
            ActiveMQQueue newReplicationQueue = new ActiveMQQueue(replicationQueueName);
            broker.getBrokerService().getBroker().addDestination(
                    broker.getAdminConnectionContext(),
                    newReplicationQueue,
                    false
            );
            logger.debug("Created replication queue {}", newReplicationQueue.getPhysicalName());
            return newReplicationQueue;
        }
    }

}
