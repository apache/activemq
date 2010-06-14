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
import java.util.List;
import java.util.Set;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;

public class SelectorAwareVirtualTopicInterceptor extends VirtualTopicInterceptor {

    public SelectorAwareVirtualTopicInterceptor(Destination next, String prefix, String postfix) {
        super(next, prefix, postfix);
    }

    /**
     * Respect the selectors of the subscriptions to ensure only matched messages are dispatched to
     * the virtual queues, hence there is no build up of unmatched messages on these destinations
     */
    @Override
    protected void send(ProducerBrokerExchange context, Message message, ActiveMQDestination destination) throws Exception {
        Broker broker = context.getConnectionContext().getBroker();
        Set<Destination> destinations = broker.getDestinations(destination);

        for (Destination dest : destinations) {
            if (matchesSomeConsumer(message, dest)) {
                dest.send(context, message.copy());
            }
        }
    }
    
    private boolean matchesSomeConsumer(Message message, Destination dest) throws IOException {
        boolean matches = false;
        MessageEvaluationContext msgContext = new NonCachedMessageEvaluationContext();
        msgContext.setDestination(dest.getActiveMQDestination());
        msgContext.setMessageReference(message);
        List<Subscription> subs = dest.getConsumers();
        for (Subscription sub: subs) {
            if (sub.matches(message, msgContext)) {
                matches = true;
                break;
            }
        }
        return matches;
    }
}
