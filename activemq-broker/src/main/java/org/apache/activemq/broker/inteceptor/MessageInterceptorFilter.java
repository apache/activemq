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
package org.apache.activemq.broker.inteceptor;

import java.util.Set;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.state.ProducerState;

class MessageInterceptorFilter extends BrokerFilter {
    private DestinationMap interceptorMap = new DestinationMap();

    MessageInterceptorFilter(Broker next) {
        super(next);
    }


    MessageInterceptor addMessageInterceptor(String destinationName, MessageInterceptor messageInterceptor) {
        ActiveMQDestination activeMQDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        interceptorMap.put(activeMQDestination, messageInterceptor);
        return messageInterceptor;
    }

    void removeMessageInterceptor(String destinationName, MessageInterceptor interceptor) {
        ActiveMQDestination activeMQDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        interceptorMap.remove(activeMQDestination, interceptor);
    }


    MessageInterceptor addMessageInterceptorForQueue(String destinationName, MessageInterceptor messageInterceptor) {
        ActiveMQDestination activeMQDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        interceptorMap.put(activeMQDestination, messageInterceptor);
        return messageInterceptor;
    }

    void removeMessageInterceptorForQueue(String destinationName, MessageInterceptor interceptor) {
        ActiveMQDestination activeMQDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        interceptorMap.remove(activeMQDestination, interceptor);
    }


    MessageInterceptor addMessageInterceptorForTopic(String destinationName, MessageInterceptor messageInterceptor) {
        ActiveMQDestination activeMQDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.TOPIC_TYPE);
        interceptorMap.put(activeMQDestination, messageInterceptor);
        return messageInterceptor;
    }

    void removeMessageInterceptorForTopic(String destinationName, MessageInterceptor interceptor) {
        ActiveMQDestination activeMQDestination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.TOPIC_TYPE);
        interceptorMap.remove(activeMQDestination, interceptor);
    }

    MessageInterceptor addMessageInterceptor(ActiveMQDestination activeMQDestination, MessageInterceptor messageInterceptor) {
        interceptorMap.put(activeMQDestination, messageInterceptor);
        return messageInterceptor;
    }

    void removeMessageInterceptor(ActiveMQDestination activeMQDestination, MessageInterceptor interceptor) {
        interceptorMap.remove(activeMQDestination, interceptor);
    }


    /**
     * Re-inject into the Broker chain
     */

    void injectMessage(ProducerBrokerExchange producerExchange, final Message messageSend) throws Exception {
        ProducerBrokerExchange pe = producerExchange;
        if (pe == null) {
            pe = new ProducerBrokerExchange();
            ConnectionContext cc = new ConnectionContext();
            cc.setBroker(this.getRoot());
            pe.setConnectionContext(cc);
            pe.setMutable(true);
            pe.setProducerState(new ProducerState(new ProducerInfo()));
        }
        super.send(pe, messageSend);
    }


    @Override
    public void send(ProducerBrokerExchange producerExchange, final Message messageSend) throws Exception {
        ActiveMQDestination activeMQDestination = messageSend.getDestination();
        if (!interceptorMap.isEmpty() && activeMQDestination != null) {
            Set<MessageInterceptor> set = interceptorMap.get(activeMQDestination);
            if (set != null && !set.isEmpty()) {
                for (MessageInterceptor mi : set) {
                    mi.intercept(producerExchange, messageSend);
                }
            } else {
                super.send(producerExchange, messageSend);
            }

        } else {
            super.send(producerExchange, messageSend);
        }
    }
}
