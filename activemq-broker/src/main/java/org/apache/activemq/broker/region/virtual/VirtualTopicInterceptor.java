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
package org.apache.activemq.broker.region.virtual;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.util.LRUCache;

import javax.jms.ResourceAllocationException;

/**
 * A Destination which implements <a href="https://activemq.apache.org/virtual-destinations">Virtual Topic</a>
 */
public class VirtualTopicInterceptor extends DestinationFilter {

    private final String prefix;
    private final String postfix;
    private final boolean local;
    private final boolean concurrentSend;
    private final boolean transactedSend;
    private final boolean dropMessageOnResourceLimit;
    private final boolean setOriginalDestination;

    private final LRUCache<ActiveMQDestination, ActiveMQQueue> cache = new LRUCache<ActiveMQDestination, ActiveMQQueue>();

    public VirtualTopicInterceptor(Destination next, VirtualTopic virtualTopic) {
        super(next);
        this.prefix = virtualTopic.getPrefix();
        this.postfix = virtualTopic.getPostfix();
        this.local = virtualTopic.isLocal();
        this.concurrentSend = virtualTopic.isConcurrentSend();
        this.transactedSend = virtualTopic.isTransactedSend();
        this.dropMessageOnResourceLimit = virtualTopic.isDropOnResourceLimit();
        this.setOriginalDestination = virtualTopic.isSetOriginalDestination();
    }

    public Topic getTopic() {
        return (Topic) this.next;
    }

    @Override
    public void send(ProducerBrokerExchange context, Message message) throws Exception {
        if (!message.isAdvisory() && !(local && message.getBrokerPath() != null)) {
            ActiveMQDestination queueConsumers = getQueueConsumersWildcard(message.getDestination());
            send(context, message, queueConsumers);
        }
        super.send(context, message);
    }

    @Override
    protected void send(final ProducerBrokerExchange context, final Message message, ActiveMQDestination destination) throws Exception {
        final Broker broker = context.getConnectionContext().getBroker();
        final Set<Destination> destinations = broker.getDestinations(destination);
        final int numDestinations = destinations.size();

        final LocalTransactionId localBrokerTransactionToCoalesceJournalSync =
                beginLocalTransaction(numDestinations, context.getConnectionContext(), message);
        try {
            if (concurrentSend && numDestinations > 1) {

                final CountDownLatch concurrent = new CountDownLatch(destinations.size());
                final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<Exception>();
                final BrokerService brokerService = broker.getBrokerService();

                for (final Destination dest : destinations) {
                    if (shouldDispatch(broker, message, dest)) {
                        brokerService.getTaskRunnerFactory().execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    if (exceptionAtomicReference.get() == null) {
                                        dest.send(context, copy(message, dest.getActiveMQDestination()));
                                    }
                                } catch (ResourceAllocationException e) {
                                    if (!dropMessageOnResourceLimit) {
                                        exceptionAtomicReference.set(e);
                                    }
                                } catch (Exception e) {
                                    exceptionAtomicReference.set(e);
                                } finally {
                                    concurrent.countDown();
                                }
                            }
                        });
                    } else {
                        concurrent.countDown();
                    }
                }
                concurrent.await();
                if (exceptionAtomicReference.get() != null) {
                    throw exceptionAtomicReference.get();
                }

            } else {
                for (final Destination dest : destinations) {
                    if (shouldDispatch(broker, message, dest)) {
                        try {
                            dest.send(context, copy(message, dest.getActiveMQDestination()));
                        } catch (ResourceAllocationException e) {
                            if (!dropMessageOnResourceLimit) {
                                throw e;
                            }
                        }
                    }
                }
            }
        } finally {
            commit(localBrokerTransactionToCoalesceJournalSync, context.getConnectionContext(), message);
        }
    }

    private Message copy(Message original, ActiveMQDestination target) {
        Message msg = original.copy();
        if (setOriginalDestination) {
            msg.setDestination(target);
            msg.setOriginalDestination(original.getDestination());
        }
        return msg;
    }

    private LocalTransactionId beginLocalTransaction(int numDestinations, ConnectionContext connectionContext, Message message) throws Exception {
        LocalTransactionId result = null;
        if (transactedSend && numDestinations > 1 && message.isPersistent() && message.getTransactionId() == null) {
            result = new LocalTransactionId(new ConnectionId(message.getMessageId().getProducerId().toString()), message.getMessageId().getProducerSequenceId());
            connectionContext.getBroker().beginTransaction(connectionContext, result);
            connectionContext.setTransaction(connectionContext.getTransactions().get(result));
            message.setTransactionId(result);
        }
        return result;
    }

    private void commit(LocalTransactionId tx, ConnectionContext connectionContext, Message message) throws Exception {
        if (tx != null) {
            connectionContext.getBroker().commitTransaction(connectionContext, tx, true);
            connectionContext.getTransactions().remove(tx);
            connectionContext.setTransaction(null);
            message.setTransactionId(null);
        }
    }

    protected boolean shouldDispatch(Broker broker, Message message, Destination dest) throws IOException {
    	//if can't find .* in the prefix, default back to old logic and return true
    	return prefix.contains(".*") && !prefix.startsWith("*") ? dest.getName().startsWith(prefix.substring(0, prefix.indexOf(".*"))) : true;
    }

    protected ActiveMQDestination getQueueConsumersWildcard(ActiveMQDestination original) {
        ActiveMQQueue queue;
        synchronized (cache) {
            queue = cache.get(original);
            if (queue == null) {
                queue = new ActiveMQQueue(prefix + original.getPhysicalName() + postfix);
                cache.put(original, queue);
            }
        }
        return queue;
    }
}
