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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.activemq.command.Command;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.command.TransactionId;

public class TransactionState {

    private final List<Command> commands = new ArrayList<Command>();
    private final TransactionId id;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private boolean prepared;
    private int preparedResult;
    private final Map<ProducerId, ProducerState> producers = new ConcurrentHashMap<ProducerId, ProducerState>();
    private final long createdAt = System.currentTimeMillis();

    public TransactionState(TransactionId id) {
        this.id = id;
    }

    public String toString() {
        return id.toString();
    }

    public void addCommand(Command operation) {
        checkShutdown();
        commands.add(operation);
    }

    public List<Command> getCommands() {
        return commands;
    }

    private void checkShutdown() {
        if (shutdown.get()) {
            throw new IllegalStateException("Disposed");
        }
    }

    public void shutdown() {
        shutdown.set(false);
    }

    public TransactionId getId() {
        return id;
    }

    public void setPrepared(boolean prepared) {
        this.prepared = prepared;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public void setPreparedResult(int preparedResult) {
        this.preparedResult = preparedResult;
    }

    public int getPreparedResult() {
        return preparedResult;
    }

    public void addProducerState(ProducerState producerState) {
        if (producerState != null) {
            producers.put(producerState.getInfo().getProducerId(), producerState);
        }
    }

    public Map<ProducerId, ProducerState> getProducerStates() {
        return producers;
    }

    public long getCreatedAt() {
        return createdAt;
    }
}
