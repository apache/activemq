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
package org.apache.activemq.junit;

import java.io.Serializable;
import java.net.URI;
import java.util.Map;
import javax.jms.BytesMessage;
import javax.jms.IllegalStateException;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;

public class ActiveMQDynamicQueueSenderResource extends AbstractActiveMQProducerResource {
    public ActiveMQDynamicQueueSenderResource(ActiveMQConnectionFactory connectionFactory) {
        super(connectionFactory);
    }

    public ActiveMQDynamicQueueSenderResource(URI brokerURI) {
        super(brokerURI);
    }

    public ActiveMQDynamicQueueSenderResource(EmbeddedActiveMQBroker embeddedActiveMQBroker) {
        super(embeddedActiveMQBroker);
    }

    public ActiveMQDynamicQueueSenderResource(URI brokerURI, String userName, String password) {
        super(brokerURI, userName, password);
    }

    public ActiveMQDynamicQueueSenderResource(String defaultDestinationName, ActiveMQConnectionFactory connectionFactory) {
        super(defaultDestinationName, connectionFactory);
    }

    public ActiveMQDynamicQueueSenderResource(String defaultDestinationName, URI brokerURI) {
        super(defaultDestinationName, brokerURI);
    }

    public ActiveMQDynamicQueueSenderResource(String destinationName, EmbeddedActiveMQBroker embeddedActiveMQBroker) {
        super(destinationName, embeddedActiveMQBroker);
    }

    public ActiveMQDynamicQueueSenderResource(String defaultDestinationName, URI brokerURI, String userName, String password) {
        super(defaultDestinationName, brokerURI, userName, password);
    }

    @Override
    protected void createClient() throws JMSException {
        producer = session.createProducer(null);
    }

    @Override
    public byte getDestinationType() {
        return ActiveMQDestination.QUEUE_TYPE;
    }

    @Override
    public void sendMessage(Message message) throws JMSException {
        if (destination == null) {
            throw new IllegalStateException("Destination is not specified");
        }

        producer.send(destination, message);
    }

    public void sendMessage(String destinationName, Message message) throws JMSException {
        producer.send(createDestination(destinationName), message);
    }

    public BytesMessage sendMessage(String destinationName, byte[] body) throws JMSException {
        BytesMessage message = this.createMessage(body);
        sendMessage(destinationName, message);
        return message;
    }

    public TextMessage sendMessage(String destinationName, String body) throws JMSException {
        TextMessage message = this.createMessage(body);
        sendMessage(destinationName, message);
        return message;
    }

    public MapMessage sendMessage(String destinationName, Map<String, Object> body) throws JMSException {
        MapMessage message = this.createMessage(body);
        sendMessage(destinationName, message);
        return message;
    }

    public ObjectMessage sendMessage(String destinationName, Serializable body) throws JMSException {
        ObjectMessage message = this.createMessage(body);
        sendMessage(destinationName, message);
        return message;
    }

    public BytesMessage sendMessageWithProperties(String destinationName, byte[] body, Map<String, Object> properties) throws JMSException {
        BytesMessage message = this.createMessage(body, properties);
        sendMessage(destinationName, message);
        return message;
    }

    public TextMessage sendMessageWithProperties(String destinationName, String body, Map<String, Object> properties) throws JMSException {
        TextMessage message = this.createMessage(body, properties);
        sendMessage(destinationName, message);
        return message;
    }

    public MapMessage sendMessageWithProperties(String destinationName, Map<String, Object> body, Map<String, Object> properties) throws JMSException {
        MapMessage message = this.createMessage(body, properties);
        sendMessage(destinationName, message);
        return message;
    }

    public ObjectMessage sendMessageWithProperties(String destinationName, Serializable body, Map<String, Object> properties) throws JMSException {
        ObjectMessage message = this.createMessage(body, properties);
        sendMessage(destinationName, message);
        return message;
    }

}
