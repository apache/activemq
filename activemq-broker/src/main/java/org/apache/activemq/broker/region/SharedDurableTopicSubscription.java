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
package org.apache.activemq.broker.region;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import jakarta.jms.JMSException;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.SubscriptionKey;

/**
 * Durable topic subscription that dispatches messages queue-style across
 * multiple consumers sharing the same subscription name.
 *
 * <p>The parent {@link DurableTopicSubscription} assumes a single consumer.
 * This subclass maintains a list of consumers and overrides {@code dispatch()}
 * to round-robin among non-full consumers, giving each message to exactly
 * one consumer — queue semantics within a topic subscription.
 */
public class SharedDurableTopicSubscription extends DurableTopicSubscription {

    private final SubscriptionKey storedKey;
    private final CopyOnWriteArrayList<ConsumerState> consumers = new CopyOnWriteArrayList<>();
    private final ConcurrentHashMap<MessageId, ConsumerId> dispatchedTo = new ConcurrentHashMap<>();
    private int nextConsumerIndex;

    public SharedDurableTopicSubscription(Broker broker, SystemUsage usageManager,
            ConnectionContext context, ConsumerInfo info, boolean keepDurableSubsActive)
            throws JMSException {
        super(broker, usageManager, context, info, keepDurableSubsActive);
        consumers.add(new ConsumerState(context, info));
        String clientId = context.getClientId() != null ? context.getClientId() : "";
        this.storedKey = new SubscriptionKey(clientId, info.getSubscriptionName());
    }

    @Override
    public SubscriptionKey getSubscriptionKey() {
        return storedKey;
    }

    public void addConsumer(ConnectionContext ctx, ConsumerInfo consumerInfo) throws IOException {
        if (!isActive() && !consumers.isEmpty()) {
            consumers.clear();
        }
        consumers.add(new ConsumerState(ctx, consumerInfo));
        dispatchPending();
    }

    public void removeConsumer(ConsumerId consumerId) throws Exception {
        ConsumerState removed = null;
        for (ConsumerState cs : consumers) {
            if (cs.info.getConsumerId().equals(consumerId)) {
                removed = cs;
                consumers.remove(cs);
                break;
            }
        }
        if (removed != null) {
            requeueDispatchedTo(removed);
            if (!consumers.isEmpty()) {
                ConsumerState first = consumers.get(0);
                this.context = first.context;
                this.info = first.info;
                dispatchPending();
            }
        }
    }

    public int getConsumerCount() {
        return consumers.size();
    }

    public boolean hasConsumers() {
        return !consumers.isEmpty();
    }

    // [dispatch override] queue-style among consumers

    @Override
    protected boolean dispatch(MessageReference node) throws IOException {
        ConsumerState target = selectConsumer();
        if (target == null) {
            return false;
        }

        ConnectionContext savedContext = this.context;
        ConsumerInfo savedInfo = this.info;
        this.context = target.context;
        this.info = target.info;
        try {
            boolean result = super.dispatch(node);
            if (result) {
                dispatchedTo.put(node.getMessageId(), target.info.getConsumerId());
                target.dispatched++;
            }
            return result;
        } finally {
            this.context = savedContext;
            this.info = savedInfo;
        }
    }

    @Override
    public boolean isFull() {
        if (!isActive()) {
            return true;
        }
        List<ConsumerState> snapshot = consumers;
        if (snapshot.isEmpty()) {
            return true;
        }
        for (ConsumerState cs : snapshot) {
            if (!cs.isFull()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int countBeforeFull() {
        int total = 0;
        for (ConsumerState cs : consumers) {
            total += cs.countBeforeFull();
        }
        return total;
    }

    // [ack routing] decrement the correct consumer's dispatch count

    @Override
    protected void acknowledge(ConnectionContext ctx, MessageAck ack,
            MessageReference node) throws IOException {
        ConsumerId cid = dispatchedTo.remove(node.getMessageId());
        if (cid != null) {
            for (ConsumerState cs : consumers) {
                if (cs.info.getConsumerId().equals(cid)) {
                    cs.dispatched--;
                    break;
                }
            }
        }
        super.acknowledge(ctx, ack, node);
    }

    // [consumer selection] round-robin, skip full

    ConsumerState selectConsumer() {
        List<ConsumerState> snapshot = consumers;
        int size = snapshot.size();
        if (size == 0) {
            return null;
        }
        for (int i = 0; i < size; i++) {
            int idx = (nextConsumerIndex + i) % size;
            ConsumerState cs = snapshot.get(idx);
            if (!cs.isFull()) {
                nextConsumerIndex = (idx + 1) % size;
                return cs;
            }
        }
        return null;
    }

    private void requeueDispatchedTo(ConsumerState removed) throws Exception {
        ConsumerId cid = removed.info.getConsumerId();
        List<MessageReference> toRequeue = new ArrayList<>();

        synchronized (dispatchLock) {
            for (MessageReference ref : dispatched) {
                ConsumerId owner = dispatchedTo.get(ref.getMessageId());
                if (cid.equals(owner)) {
                    toRequeue.add(ref);
                }
            }
            for (MessageReference ref : toRequeue) {
                dispatched.remove(ref);
                dispatchedTo.remove(ref.getMessageId());
            }
        }

        if (!toRequeue.isEmpty()) {
            Collections.reverse(toRequeue);
            synchronized (pendingLock) {
                for (MessageReference ref : toRequeue) {
                    ref.incrementRedeliveryCounter();
                    pending.addMessageFirst(ref);
                }
            }
        }
    }

    // [per-consumer state]

    static class ConsumerState {
        final ConnectionContext context;
        final ConsumerInfo info;
        int dispatched;

        ConsumerState(ConnectionContext context, ConsumerInfo info) {
            this.context = context;
            this.info = info;
        }

        boolean isFull() {
            return info.getPrefetchSize() > 0 && dispatched >= info.getPrefetchSize();
        }

        int countBeforeFull() {
            return Math.max(0, info.getPrefetchSize() - dispatched);
        }
    }
}
