/*
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

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.DestinationFilter;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;

/**
 * Represents a composite {@link Destination} where send()s are replicated to
 * each Destination instance.
 */
public class CompositeDestinationFilter extends DestinationFilter {

    private Collection forwardDestinations;
    private boolean forwardOnly;
    private boolean concurrentSend = false;

    public CompositeDestinationFilter(Destination next, Collection forwardDestinations, boolean forwardOnly, boolean concurrentSend) {
        super(next);
        this.forwardDestinations = forwardDestinations;
        this.forwardOnly = forwardOnly;
        this.concurrentSend = concurrentSend;
    }

    @Override
    public void send(final ProducerBrokerExchange context, final Message message) throws Exception {
        MessageEvaluationContext messageContext = null;

        Collection<ActiveMQDestination> matchingDestinations = new LinkedList<ActiveMQDestination>();
        for (Iterator iter = forwardDestinations.iterator(); iter.hasNext();) {
            ActiveMQDestination destination = null;
            Object value = iter.next();

            if (value instanceof FilteredDestination) {
                FilteredDestination filteredDestination = (FilteredDestination)value;
                if (messageContext == null) {
                    messageContext = new NonCachedMessageEvaluationContext();
                    messageContext.setMessageReference(message);
                }
                messageContext.setDestination(filteredDestination.getDestination());
                if (filteredDestination.matches(messageContext)) {
                    destination = filteredDestination.getDestination();
                }
            } else if (value instanceof ActiveMQDestination) {
                destination = (ActiveMQDestination)value;
            }
            if (destination == null) {
                continue;
            }
            matchingDestinations.add(destination);
        }

        final CountDownLatch concurrent = new CountDownLatch(concurrentSend ? matchingDestinations.size() : 0);
        final AtomicReference<Exception> exceptionAtomicReference = new AtomicReference<Exception>();
        final BrokerService brokerService = context.getConnectionContext().getBroker().getBrokerService();
        for (final ActiveMQDestination destination : matchingDestinations) {
            if (concurrent.getCount() > 0) {
                brokerService.getTaskRunnerFactory().execute(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            if (exceptionAtomicReference.get() == null) {
                                doForward(context.copy(), message, brokerService.getRegionBroker(), destination);
                            }
                        } catch (Exception e) {
                            exceptionAtomicReference.set(e);
                        } finally {
                            concurrent.countDown();
                        }
                    }
                });
            } else {
                doForward(context, message, brokerService.getRegionBroker(), destination);
            }
        }
        if (!forwardOnly) {
            super.send(context, message);
        }
        concurrent.await();
        if (exceptionAtomicReference.get() != null) {
            throw exceptionAtomicReference.get();
        }
    }

    private void doForward(ProducerBrokerExchange context, Message message, Broker regionBroker, ActiveMQDestination destination) throws Exception {
        Message forwardedMessage = message.copy();
        forwardedMessage.setMemoryUsage(null);

        forwardedMessage.setOriginalDestination( message.getDestination() );
        forwardedMessage.setDestination(destination);

        // Send it back through the region broker for routing.
        context.setMutable(true);
        regionBroker.send(context, forwardedMessage);
    }
}
