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

import java.net.URI;
import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;

public abstract class AbstractActiveMQConsumerResource extends AbstractActiveMQClientResource {
    MessageConsumer consumer;
    long defaultReceiveTimout = 50;

    public AbstractActiveMQConsumerResource(String destinationName, ActiveMQConnectionFactory connectionFactory) {
        super(destinationName, connectionFactory);
    }

    public AbstractActiveMQConsumerResource(String destinationName, URI brokerURI) {
        super(destinationName, brokerURI);
    }

    public AbstractActiveMQConsumerResource(String destinationName, EmbeddedActiveMQBroker embeddedActiveMQBroker) {
        super(destinationName, embeddedActiveMQBroker);
    }

    public AbstractActiveMQConsumerResource(String destinationName, URI brokerURI, String userName, String password) {
        super(destinationName, brokerURI, userName, password);
    }

    public long getDefaultReceiveTimout() {
        return defaultReceiveTimout;
    }

    public void setDefaultReceiveTimout(long defaultReceiveTimout) {
        this.defaultReceiveTimout = defaultReceiveTimout;
    }

    @Override
    protected void createClient() throws JMSException {
        consumer = session.createConsumer(destination);
    }

    public BytesMessage receiveBytesMessage() throws JMSException {
        return (BytesMessage) this.receiveMessage();
    }

    public TextMessage receiveTextMessage() throws JMSException {
        return (TextMessage) this.receiveMessage();
    }

    public MapMessage receiveMapMessage() throws JMSException {
        return (MapMessage) this.receiveMessage();
    }

    public ObjectMessage receiveObjectMessage() throws JMSException {
        return (ObjectMessage) this.receiveMessage();
    }

    public BytesMessage receiveBytesMessage(long timeout) throws JMSException {
        return (BytesMessage) this.receiveMessage(timeout);
    }

    public TextMessage receiveTextMessage(long timeout) throws JMSException {
        return (TextMessage) this.receiveMessage(timeout);
    }

    public MapMessage receiveMapMessage(long timeout) throws JMSException {
        return (MapMessage) this.receiveMessage(timeout);
    }

    public ObjectMessage receiveObjectMessage(long timeout) throws JMSException {
        return (ObjectMessage) this.receiveMessage(timeout);
    }

    public Message receiveMessage() throws JMSException {
        return receiveMessage(defaultReceiveTimout);
    }

    /**
     * Receive a message with the given timeout
     *
     * @param timeout
     * @return
     * @throws JMSException
     */
    public Message receiveMessage(long timeout) throws JMSException {
        Message message = null;
        if (timeout > 0) {
            message = consumer.receive(timeout);
        } else if (timeout == 0) {
            message = consumer.receiveNoWait();
        } else {
            message = consumer.receive();
        }

        return message;
    }
}
