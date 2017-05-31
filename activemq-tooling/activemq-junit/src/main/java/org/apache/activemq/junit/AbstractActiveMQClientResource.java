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
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractActiveMQClientResource extends ExternalResource {
    Logger log = LoggerFactory.getLogger(this.getClass());

    ActiveMQConnectionFactory connectionFactory;
    Connection connection;
    Session session;
    ActiveMQDestination destination;

    public AbstractActiveMQClientResource(ActiveMQConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    public AbstractActiveMQClientResource(URI brokerURI) {
        this(new ActiveMQConnectionFactory(brokerURI));
    }

    public AbstractActiveMQClientResource(EmbeddedActiveMQBroker embeddedActiveMQBroker) {
        this(embeddedActiveMQBroker.createConnectionFactory());
    }

    public AbstractActiveMQClientResource(URI brokerURI, String userName, String password) {
        this(new ActiveMQConnectionFactory(userName, password, brokerURI));
    }

    public AbstractActiveMQClientResource(String destinationName, ActiveMQConnectionFactory connectionFactory) {
        this(connectionFactory);
        destination = createDestination(destinationName);
    }

    public AbstractActiveMQClientResource(String destinationName, URI brokerURI) {
        this(destinationName, new ActiveMQConnectionFactory(brokerURI));
    }

    public AbstractActiveMQClientResource(String destinationName, EmbeddedActiveMQBroker embeddedActiveMQBroker) {
        this(destinationName, embeddedActiveMQBroker.createConnectionFactory());
    }

    public AbstractActiveMQClientResource(String destinationName, URI brokerURI, String userName, String password) {
        this(destinationName, new ActiveMQConnectionFactory(userName, password, brokerURI));
    }

    public static void setMessageProperties(Message message, Map<String, Object> properties) throws JMSException {
        if (properties != null) {
            for (Map.Entry<String, Object> property : properties.entrySet()) {
                message.setObjectProperty(property.getKey(), property.getValue());
            }
        }
    }

    public String getClientId() {
        return null;
    }

    public String getDestinationName() {
        return (destination != null) ? destination.toString() : null;
    }

    public abstract byte getDestinationType();

    protected abstract void createClient() throws JMSException;

    /**
     * Start the Client
     * <p/>
     * Invoked by JUnit to setup the resource
     */
    @Override
    protected void before() throws Throwable {
        log.info("Starting {}: {}", this.getClass().getSimpleName(), connectionFactory.getBrokerURL());

        this.start();

        super.before();
    }

    /**
     * Stop the Client
     * <p/>
     * Invoked by JUnit to tear down the resource
     */
    @Override
    protected void after() {
        log.info("Stopping {}: {}", this.getClass().getSimpleName(), connectionFactory.getBrokerURL());

        super.after();

        this.stop();
    }

    public void start() {
        try {
            try {
                connection = connectionFactory.createConnection();
                String clientId = getClientId();
                if (clientId != null) {
                    connection.setClientID(clientId);
                }
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                createClient();
            } catch (JMSException jmsEx) {
                throw new RuntimeException("Producer initialization failed" + this.getClass().getSimpleName(), jmsEx);
            }
            connection.start();
        } catch (JMSException jmsEx) {
            throw new IllegalStateException("Producer failed to start", jmsEx);
        }
        log.info("Ready to produce messages to {}", connectionFactory.getBrokerURL());
    }

    public void stop() {
        try {
            connection.close();
        } catch (JMSException jmsEx) {
            log.warn("Exception encountered closing JMS Connection", jmsEx);
        }
    }

    public String getBrokerURL() {
        return connectionFactory.getBrokerURL();
    }

    protected ActiveMQDestination createDestination(String destinationName) {
        if (destinationName != null) {
            return ActiveMQDestination.createDestination(destinationName, getDestinationType());
        }

        return null;
    }

    public BytesMessage createBytesMessage() throws JMSException {
        return session.createBytesMessage();
    }

    public TextMessage createTextMessage() throws JMSException {
        return session.createTextMessage();
    }

    public MapMessage createMapMessage() throws JMSException {
        return session.createMapMessage();
    }

    public ObjectMessage createObjectMessage() throws JMSException {
        return session.createObjectMessage();
    }

    public StreamMessage createStreamMessage() throws JMSException {
        return session.createStreamMessage();
    }

    public BytesMessage createMessage(byte[] body) throws JMSException {
        return this.createMessage(body, null);
    }

    public TextMessage createMessage(String body) throws JMSException {
        return this.createMessage(body, null);
    }

    public MapMessage createMessage(Map<String, Object> body) throws JMSException {
        return this.createMessage(body, null);
    }

    public ObjectMessage createMessage(Serializable body) throws JMSException {
        return this.createMessage(body, null);
    }

    public BytesMessage createMessage(byte[] body, Map<String, Object> properties) throws JMSException {
        BytesMessage message = this.createBytesMessage();
        if (body != null) {
            message.writeBytes(body);
        }

        setMessageProperties(message, properties);

        return message;
    }

    public TextMessage createMessage(String body, Map<String, Object> properties) throws JMSException {
        TextMessage message = this.createTextMessage();
        if (body != null) {
            message.setText(body);
        }

        setMessageProperties(message, properties);

        return message;
    }

    public MapMessage createMessage(Map<String, Object> body, Map<String, Object> properties) throws JMSException {
        MapMessage message = this.createMapMessage();

        if (body != null) {
            for (Map.Entry<String, Object> entry : body.entrySet()) {
                message.setObject(entry.getKey(), entry.getValue());
            }
        }

        setMessageProperties(message, properties);

        return message;
    }

    public ObjectMessage createMessage(Serializable body, Map<String, Object> properties) throws JMSException {
        ObjectMessage message = this.createObjectMessage();

        if (body != null) {
            message.setObject(body);
        }

        setMessageProperties(message, properties);

        return message;
    }
}
