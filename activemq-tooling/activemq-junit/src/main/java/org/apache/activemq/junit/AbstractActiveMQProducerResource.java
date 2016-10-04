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
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public abstract class AbstractActiveMQProducerResource extends AbstractActiveMQClientResource {
    MessageProducer producer;

    public AbstractActiveMQProducerResource(ActiveMQConnectionFactory connectionFactory) {
        super(connectionFactory);
    }

    public AbstractActiveMQProducerResource(URI brokerURI) {
        super(brokerURI);
    }

    public AbstractActiveMQProducerResource(EmbeddedActiveMQBroker embeddedActiveMQBroker) {
        super(embeddedActiveMQBroker);
    }

    public AbstractActiveMQProducerResource(URI brokerURI, String userName, String password) {
        super(brokerURI, userName, password);
    }

    public AbstractActiveMQProducerResource(String destinationName, ActiveMQConnectionFactory connectionFactory) {
        super(destinationName, connectionFactory);
    }

    public AbstractActiveMQProducerResource(String destinationName, URI brokerURI) {
        super(destinationName, brokerURI);
    }

    public AbstractActiveMQProducerResource(String destinationName, EmbeddedActiveMQBroker embeddedActiveMQBroker) {
        super(destinationName, embeddedActiveMQBroker);
    }

    public AbstractActiveMQProducerResource(String destinationName, URI brokerURI, String userName, String password) {
        super(destinationName, brokerURI, userName, password);
    }

    @Override
    public String getDestinationName() {
        try {
            if (producer != null && producer.getDestination() != null) {
                return producer.getDestination().toString();
            }
        } catch (JMSException e) {
            // eat this
        }

        return null;
    }

    public void sendMessage(Message message) throws JMSException {
        producer.send(message);
    }

    public BytesMessage sendMessage(byte[] body) throws JMSException {
        BytesMessage message = this.createMessage(body);
        sendMessage(message);
        return message;
    }

    public TextMessage sendMessage(String body) throws JMSException {
        TextMessage message = this.createMessage(body);
        sendMessage(message);
        return message;
    }

    public MapMessage sendMessage(Map<String, Object> body) throws JMSException {
        MapMessage message = this.createMessage(body);
        sendMessage(message);
        return message;
    }

    public ObjectMessage sendMessage(Serializable body) throws JMSException {
        ObjectMessage message = this.createMessage(body);
        sendMessage(message);
        return message;
    }

    public BytesMessage sendMessageWithProperties(byte[] body, Map<String, Object> properties) throws JMSException {
        BytesMessage message = this.createMessage(body, properties);
        sendMessage(message);
        return message;
    }

    public TextMessage sendMessageWithProperties(String body, Map<String, Object> properties) throws JMSException {
        TextMessage message = this.createMessage(body, properties);
        sendMessage(message);
        return message;
    }

    public MapMessage sendMessageWithProperties(Map<String, Object> body, Map<String, Object> properties) throws JMSException {
        MapMessage message = this.createMessage(body, properties);
        sendMessage(message);
        return message;
    }

    public ObjectMessage sendMessageWithProperties(Serializable body, Map<String, Object> properties) throws JMSException {
        ObjectMessage message = this.createMessage(body, properties);
        sendMessage(message);
        return message;
    }

}
