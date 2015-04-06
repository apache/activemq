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
package org.apache.activemq.camel.component.broker;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.inteceptor.MessageInterceptor;
import org.apache.activemq.broker.inteceptor.MessageInterceptorRegistry;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.camel.Consumer;
import org.apache.camel.MultipleConsumersSupport;
import org.apache.camel.Processor;
import org.apache.camel.Producer;
import org.apache.camel.Service;
import org.apache.camel.api.management.ManagedResource;
import org.apache.camel.impl.DefaultEndpoint;
import org.apache.camel.spi.Metadata;
import org.apache.camel.spi.UriEndpoint;
import org.apache.camel.spi.UriParam;
import org.apache.camel.spi.UriPath;
import org.apache.camel.util.UnsafeUriCharactersEncoder;

@ManagedResource(description = "Managed Camel Broker Endpoint")
@UriEndpoint(scheme = "broker", syntax = "broker:destination", consumerClass = BrokerConsumer.class, title = "Broker", label = "messaging")
public class BrokerEndpoint extends DefaultEndpoint implements MultipleConsumersSupport, Service {

    static final String PRODUCER_BROKER_EXCHANGE = "producerBrokerExchange";

    private MessageInterceptorRegistry messageInterceptorRegistry;
    private List<MessageInterceptor> messageInterceptorList = new CopyOnWriteArrayList<MessageInterceptor>();

    @UriPath(name = "destination") @Metadata(required = "true")
    private String destinationName;
    private final ActiveMQDestination destination;
    @UriParam
    private final BrokerConfiguration configuration;

    public BrokerEndpoint(String uri, BrokerComponent component, String destinationName, ActiveMQDestination destination, BrokerConfiguration configuration) {
        super(UnsafeUriCharactersEncoder.encode(uri), component);
        this.destinationName = destinationName;
        this.destination = destination;
        this.configuration = configuration;
    }

    @Override
    public Producer createProducer() throws Exception {
        BrokerProducer producer = new BrokerProducer(this);
        return producer;
    }

    @Override
    public Consumer createConsumer(Processor processor) throws Exception {
        BrokerConsumer consumer = new BrokerConsumer(this, processor);
        configureConsumer(consumer);
        return consumer;
    }

    @Override
    public boolean isSingleton() {
        return false;
    }

    @Override
    public boolean isMultipleConsumersSupported() {
        return true;
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    /**
     * The name of the JMS destination
     */
    public String getDestinationName() {
        return destinationName;
    }

    @Override
    protected void doStart() throws Exception {
        super.doStart();
        messageInterceptorRegistry = MessageInterceptorRegistry.getInstance().get(configuration.getBrokerName());
        for (MessageInterceptor messageInterceptor : messageInterceptorList) {
            addMessageInterceptor(messageInterceptor);
        }
        messageInterceptorList.clear();
    }

    @Override
    protected void doStop() throws Exception {
        super.doStop();
    }

    protected void addMessageInterceptor(MessageInterceptor messageInterceptor) {
        if (isStarted()) {
            messageInterceptorRegistry.addMessageInterceptor(destination, messageInterceptor);
        } else {
            messageInterceptorList.add(messageInterceptor);
        }
    }

    protected void removeMessageInterceptor(MessageInterceptor messageInterceptor) {
        messageInterceptorRegistry.removeMessageInterceptor(destination, messageInterceptor);
    }

    protected void inject(ProducerBrokerExchange producerBrokerExchange, Message message) throws Exception {
        ProducerBrokerExchange pbe = producerBrokerExchange;
        if (message != null) {
            message.setDestination(destination);
            if (producerBrokerExchange != null && producerBrokerExchange.getRegionDestination() != null){
                if (!producerBrokerExchange.getRegionDestination().getActiveMQDestination().equals(destination)){
                     //The message broker will create a new ProducerBrokerExchange with the
                     //correct region broker set
                     pbe = null;
                }
            }

            messageInterceptorRegistry.injectMessage(pbe, message);
        }
    }
}
