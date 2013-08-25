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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.MutableBrokerFilter;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MessageInterceptorRegistry {
    private static final Logger LOG = LoggerFactory.getLogger(MessageInterceptorRegistry.class);
    private final BrokerService brokerService;
    private MessageInterceptorFilter filter;


    public MessageInterceptorRegistry(BrokerService brokerService) {
        this.brokerService = brokerService;
    }

    public MessageInterceptor addMessageInterceptor(String destinationName, MessageInterceptor messageInterceptor) {
        return getFilter().addMessageInterceptor(destinationName, messageInterceptor);
    }

    public void removeMessageInterceptor(String destinationName, MessageInterceptor messageInterceptor) {
        getFilter().removeMessageInterceptor(destinationName, messageInterceptor);
    }


    public MessageInterceptor addMessageInterceptorForQueue(String destinationName, MessageInterceptor messageInterceptor) {
        return getFilter().addMessageInterceptorForQueue(destinationName, messageInterceptor);
    }

    public void removeMessageInterceptorForQueue(String destinationName, MessageInterceptor messageInterceptor) {
        getFilter().addMessageInterceptorForQueue(destinationName, messageInterceptor);
    }


    public MessageInterceptor addMessageInterceptorForTopic(String destinationName, MessageInterceptor messageInterceptor) {
        return getFilter().addMessageInterceptorForTopic(destinationName, messageInterceptor);
    }

    public void removeMessageInterceptorForTopic(String destinationName, MessageInterceptor messageInterceptor) {
        getFilter().removeMessageInterceptorForTopic(destinationName, messageInterceptor);
    }

    public MessageInterceptor addMessageInterceptor(ActiveMQDestination activeMQDestination, MessageInterceptor messageInterceptor) {
        return getFilter().addMessageInterceptor(activeMQDestination, messageInterceptor);
    }

    public void removeMessageInterceptor(ActiveMQDestination activeMQDestination, MessageInterceptor interceptor) {
        getFilter().removeMessageInterceptor(activeMQDestination, interceptor);
    }

    /**
     * Re-inject into the Broker chain
     */

    public void injectMessage(ProducerBrokerExchange producerExchange, final Message messageSend) throws Exception {
        getFilter().injectMessage(producerExchange, messageSend);
    }


    private synchronized MessageInterceptorFilter getFilter() {
        if (filter == null) {
            try {
                MutableBrokerFilter mutableBrokerFilter = (MutableBrokerFilter) brokerService.getBroker().getAdaptor(MutableBrokerFilter.class);
                Broker next = mutableBrokerFilter.getNext();
                filter = new MessageInterceptorFilter(next);
                mutableBrokerFilter.setNext(filter);
            } catch (Exception e) {
                LOG.error("Failed to create MessageInterceptorFilter", e);
            }
        }
        return filter;
    }
}
