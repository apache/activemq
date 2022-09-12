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
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class ReplicaReplicationQueueSupplier implements Supplier<ActiveMQQueue> {

    private final Logger logger = LoggerFactory.getLogger(ReplicaSourceBroker.class);
    private final CountDownLatch initializationLatch = new CountDownLatch(1);
    private ActiveMQQueue replicationQueue = null; // memoized
    private final Broker broker;

    public ReplicaReplicationQueueSupplier(final Broker broker) {
        this.broker = requireNonNull(broker);
    }

    @Override
    public ActiveMQQueue get() {
        try {
            if (initializationLatch.await(1L, TimeUnit.MINUTES)) {
                return requireNonNull(replicationQueue);
            }
        } catch (InterruptedException e) {
            throw new ActiveMQReplicaException("Interrupted while waiting for replication queue initialization", e);
        }
        throw new ActiveMQReplicaException("Timed out waiting for replication queue initialization");
    }

    public void initialize() {
        try {
            replicationQueue = getOrCreateReplicationQueue();
        } catch (Exception e) {
            logger.error("Could not obtain replication queue", e);
            throw new ActiveMQReplicaException("Failed to get or create replication queue");
        }
        initializationLatch.countDown();
    }

    private ActiveMQQueue getOrCreateReplicationQueue() throws Exception {
        Optional<ActiveMQDestination> existingReplicationQueue = broker.getDurableDestinations()
            .stream()
            .filter(ActiveMQDestination::isQueue)
            .filter(d -> ReplicaSupport.REPLICATION_QUEUE_NAME.equals(d.getPhysicalName()))
            .findFirst();
        if (existingReplicationQueue.isPresent()) {
            logger.debug("Existing replication queue {}", existingReplicationQueue.get().getPhysicalName());
            return new ActiveMQQueue(existingReplicationQueue.get().getPhysicalName());
        } else {
            String mirrorQueueName = ReplicaSupport.REPLICATION_QUEUE_NAME;
            ActiveMQQueue newReplicationQueue = new ActiveMQQueue(mirrorQueueName);
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
