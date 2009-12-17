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

package org.apache.activemq.state;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;

public class SessionState {
    final SessionInfo info;

    private final Map<ProducerId, ProducerState> producers = new ConcurrentHashMap<ProducerId, ProducerState>();
    private final Map<ConsumerId, ConsumerState> consumers = new ConcurrentHashMap<ConsumerId, ConsumerState>();
    private final AtomicBoolean shutdown = new AtomicBoolean(false);

    public SessionState(SessionInfo info) {
        this.info = info;
    }

    public String toString() {
        return info.toString();
    }

    public void addProducer(ProducerInfo info) {
        checkShutdown();
        producers.put(info.getProducerId(), new ProducerState(info));
    }

    public ProducerState removeProducer(ProducerId id) {
        ProducerState producerState = producers.remove(id);
        if (producerState != null) {
            if (producerState.getTransactionState() != null) {
                // allow the transaction to recreate dependent producer on recovery
                producerState.getTransactionState().addProducerState(producerState);
            }
        }
        return producerState;
    }
    
    public void addConsumer(ConsumerInfo info) {
        checkShutdown();
        consumers.put(info.getConsumerId(), new ConsumerState(info));
    }

    public ConsumerState removeConsumer(ConsumerId id) {
        return consumers.remove(id);
    }

    public SessionInfo getInfo() {
        return info;
    }

    public Set<ConsumerId> getConsumerIds() {
        return consumers.keySet();
    }

    public Set<ProducerId> getProducerIds() {
        return producers.keySet();
    }

    public Collection<ProducerState> getProducerStates() {
        return producers.values();
    }

    public ProducerState getProducerState(ProducerId producerId) {
        return producers.get(producerId);
    }

    public Collection<ConsumerState> getConsumerStates() {
        return consumers.values();
    }

    public ConsumerState getConsumerState(ConsumerId consumerId) {
        return consumers.get(consumerId);
    }

    private void checkShutdown() {
        if (shutdown.get()) {
            throw new IllegalStateException("Disposed");
        }
    }

    public void shutdown() {
        shutdown.set(false);
    }

}
