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

import static org.apache.activemq.command.ActiveMQDestination.QUEUE_TYPE;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.plugin.StatisticsBrokerPlugin;
import org.apache.activemq.pool.PooledConnectionFactory;
import org.junit.rules.ExternalResource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JUnit Rule that embeds an ActiveMQ broker into a test.
 */
public class EmbeddedActiveMQBroker extends ExternalResource {
    Logger log = LoggerFactory.getLogger(this.getClass());

    BrokerService brokerService;
    InternalClient internalClient;

    /**
     * Create an embedded ActiveMQ broker using defaults
     * <p>
     * The defaults are:
     * - the broker name is 'embedded-broker'
     * - JMX is enable but no management connector is created.
     * - Persistence is disabled
     */
    public EmbeddedActiveMQBroker() {
        brokerService = new BrokerService();
        brokerService.setUseJmx(true);
        brokerService.getManagementContext().setCreateConnector(false);
        brokerService.setUseShutdownHook(false);
        brokerService.setPersistent(false);
        brokerService.setBrokerName("embedded-broker");
    }

    /**
     * Create an embedded ActiveMQ broker using a configuration URI
     */
    public EmbeddedActiveMQBroker(String configurationURI) {
        try {
            brokerService = BrokerFactory.createBroker(configurationURI);
        } catch (Exception ex) {
            throw new RuntimeException("Exception encountered creating embedded ActiveMQ broker from configuration URI: " + configurationURI, ex);
        }
    }

    /**
     * Create an embedded ActiveMQ broker using a configuration URI
     */
    public EmbeddedActiveMQBroker(URI configurationURI) {
        try {
            brokerService = BrokerFactory.createBroker(configurationURI);
        } catch (Exception ex) {
            throw new RuntimeException("Exception encountered creating embedded ActiveMQ broker from configuration URI: " + configurationURI, ex);
        }
    }

    public static void setMessageProperties(Message message, Map<String, Object> properties) {
        if (properties != null && properties.size() > 0) {
            for (Map.Entry<String, Object> property : properties.entrySet()) {
                try {
                    message.setObjectProperty(property.getKey(), property.getValue());
                } catch (JMSException jmsEx) {
                    throw new EmbeddedActiveMQBrokerException(String.format("Failed to set property {%s = %s}", property.getKey(), property.getValue().toString()), jmsEx);
                }
            }
        }
    }

    /**
     * Customize the configuration of the embedded ActiveMQ broker
     * <p>
     * This method is called before the embedded ActiveMQ broker is started, and can
     * be overridden to this method to customize the broker configuration.
     */
    protected void configure() {
    }

    /**
     * Start the embedded ActiveMQ broker, blocking until the broker has successfully started.
     * <p/>
     * The broker will normally be started by JUnit using the before() method.  This method allows the broker to
     * be started manually to support advanced testing scenarios.
     */
    public void start() {
        try {
            this.configure();
            brokerService.start();
            internalClient = new InternalClient();
            internalClient.start();
        } catch (Exception ex) {
            throw new RuntimeException("Exception encountered starting embedded ActiveMQ broker: {}" + this.getBrokerName(), ex);
        }

        brokerService.waitUntilStarted();
    }

    /**
     * Stop the embedded ActiveMQ broker, blocking until the broker has stopped.
     * <p/>
     * The broker will normally be stopped by JUnit using the after() method.  This method allows the broker to
     * be stopped manually to support advanced testing scenarios.
     */
    public void stop() {
        if (internalClient != null) {
            internalClient.stop();
            internalClient = null;
        }
        if (!brokerService.isStopped()) {
            try {
                brokerService.stop();
            } catch (Exception ex) {
                log.warn("Exception encountered stopping embedded ActiveMQ broker: {}" + this.getBrokerName(), ex);
            }
        }

        brokerService.waitUntilStopped();
    }

    /**
     * Start the embedded ActiveMQ Broker
     * <p/>
     * Invoked by JUnit to setup the resource
     */
    @Override
    protected void before() throws Throwable {
        log.info("Starting embedded ActiveMQ broker: {}", this.getBrokerName());

        this.start();

        super.before();
    }

    /**
     * Stop the embedded ActiveMQ Broker
     * <p/>
     * Invoked by JUnit to tear down the resource
     */
    @Override
    protected void after() {
        log.info("Stopping Embedded ActiveMQ Broker: {}", this.getBrokerName());

        super.after();

        this.stop();
    }

    /**
     * Create an ActiveMQConnectionFactory for the embedded ActiveMQ Broker
     *
     * @return a new ActiveMQConnectionFactory
     */
    public ActiveMQConnectionFactory createConnectionFactory() {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory();
        connectionFactory.setBrokerURL(getVmURL());
        return connectionFactory;
    }

    /**
     * Create an PooledConnectionFactory for the embedded ActiveMQ Broker
     *
     * @return a new PooledConnectionFactory
     */
    public PooledConnectionFactory createPooledConnectionFactory() {
        ActiveMQConnectionFactory connectionFactory = createConnectionFactory();

        PooledConnectionFactory pooledConnectionFactory = new PooledConnectionFactory(connectionFactory);

        return pooledConnectionFactory;
    }

    /**
     * Get the BrokerService for the embedded ActiveMQ broker.
     * <p/>
     * This may be required for advanced configuration of the BrokerService.
     *
     * @return the embedded ActiveMQ broker
     */
    public BrokerService getBrokerService() {
        return brokerService;
    }

    /**
     * Get the failover VM URL for the embedded ActiveMQ Broker
     * <p/>
     * NOTE:  The create=false option is appended to the URL to avoid the automatic creation of brokers
     * and the resulting duplicate broker errors
     *
     * @return the VM URL for the embedded broker
     */
    public String getVmURL() {
        return getVmURL(true);
    }

    /**
     * Get the VM URL for the embedded ActiveMQ Broker
     * <p/>
     * NOTE:  The create=false option is appended to the URL to avoid the automatic creation of brokers
     * and the resulting duplicate broker errors
     *
     * @param failoverURL if true a failover URL will be returned
     * @return the VM URL for the embedded broker
     */
    public String getVmURL(boolean failoverURL) {
        if (failoverURL) {
            return String.format("failover:(%s?create=false)", brokerService.getVmConnectorURI().toString());
        }

        return brokerService.getVmConnectorURI().toString() + "?create=false";
    }

    /**
     * Get the failover VM URI for the embedded ActiveMQ Broker
     * <p/>
     * NOTE:  The create=false option is appended to the URI to avoid the automatic creation of brokers
     * and the resulting duplicate broker errors
     *
     * @return the VM URI for the embedded broker
     */
    public URI getVmURI() {
        return getVmURI(true);
    }

    /**
     * Get the VM URI for the embedded ActiveMQ Broker
     * <p/>
     * NOTE:  The create=false option is appended to the URI to avoid the automatic creation of brokers
     * and the resulting duplicate broker errors
     *
     * @param failoverURI if true a failover URI will be returned
     * @return the VM URI for the embedded broker
     */
    public URI getVmURI(boolean failoverURI) {
        URI result;
        try {
            result = new URI(getVmURL(failoverURI));
        } catch (URISyntaxException uriEx) {
            throw new RuntimeException("Unable to create failover URI", uriEx);
        }

        return result;
    }

    /**
     * Get the name of the embedded ActiveMQ Broker
     *
     * @return name of the embedded broker
     */
    public String getBrokerName() {
        return brokerService.getBrokerName();
    }

    public void setBrokerName(String brokerName) {
        brokerService.setBrokerName(brokerName);
    }

    public boolean isStatisticsPluginEnabled() {
        BrokerPlugin[] plugins = brokerService.getPlugins();

        if (null != plugins) {
            for (BrokerPlugin plugin : plugins) {
                if (plugin instanceof StatisticsBrokerPlugin) {
                    return true;
                }
            }
        }

        return false;
    }

    public void enableStatisticsPlugin() {
        if (!isStatisticsPluginEnabled()) {
            BrokerPlugin[] newPlugins;
            BrokerPlugin[] currentPlugins = brokerService.getPlugins();
            if (null != currentPlugins && 0 < currentPlugins.length) {
                newPlugins = new BrokerPlugin[currentPlugins.length + 1];

                System.arraycopy(currentPlugins, 0, newPlugins, 0, currentPlugins.length);
            } else {
                newPlugins = new BrokerPlugin[1];
            }

            newPlugins[newPlugins.length - 1] = new StatisticsBrokerPlugin();

            brokerService.setPlugins(newPlugins);
        }
    }

    public void disableStatisticsPlugin() {
        if (isStatisticsPluginEnabled()) {
            BrokerPlugin[] currentPlugins = brokerService.getPlugins();
            if (1 < currentPlugins.length) {
                BrokerPlugin[] newPlugins = new BrokerPlugin[currentPlugins.length - 1];

                int i = 0;
                for (BrokerPlugin plugin : currentPlugins) {
                    if (!(plugin instanceof StatisticsBrokerPlugin)) {
                        newPlugins[i++] = plugin;
                    }
                }
                brokerService.setPlugins(newPlugins);
            } else {
                brokerService.setPlugins(null);
            }

        }
    }

    public boolean isAdvisoryForDeliveryEnabled() {
        return getDefaultPolicyEntry().isAdvisoryForDelivery();
    }

    public void enableAdvisoryForDelivery() {
        getDefaultPolicyEntry().setAdvisoryForDelivery(true);
    }

    public void disableAdvisoryForDelivery() {
        getDefaultPolicyEntry().setAdvisoryForDelivery(false);
    }

    public boolean isAdvisoryForConsumedEnabled() {
        return getDefaultPolicyEntry().isAdvisoryForConsumed();
    }

    public void enableAdvisoryForConsumed() {
        getDefaultPolicyEntry().setAdvisoryForConsumed(true);
    }

    public void disableAdvisoryForConsumed() {
        getDefaultPolicyEntry().setAdvisoryForConsumed(false);
    }

    public boolean isAdvisoryForDiscardingMessagesEnabled() {
        return getDefaultPolicyEntry().isAdvisoryForDiscardingMessages();
    }

    public void enableAdvisoryForDiscardingMessages() {
        getDefaultPolicyEntry().setAdvisoryForDiscardingMessages(true);
    }

    public void disableAdvisoryForDiscardingMessages() {
        getDefaultPolicyEntry().setAdvisoryForDiscardingMessages(false);
    }

    public boolean isAdvisoryForFastProducersEnabled() {
        return getDefaultPolicyEntry().isAdvisoryForFastProducers();
    }

    public void enableAdvisoryForFastProducers() {
        getDefaultPolicyEntry().setAdvisoryForFastProducers(true);
    }

    public void disableAdvisoryForFastProducers() {
        getDefaultPolicyEntry().setAdvisoryForFastProducers(false);
    }

    public boolean isAdvisoryForSlowConsumersEnabled() {
        return getDefaultPolicyEntry().isAdvisoryForSlowConsumers();
    }

    public void enableAdvisoryForSlowConsumers() {
        getDefaultPolicyEntry().setAdvisoryForSlowConsumers(true);
    }

    public void disableAdvisoryForSlowConsumers() {
        getDefaultPolicyEntry().setAdvisoryForSlowConsumers(false);
    }

    /**
     * Get the number of messages in a specific JMS Destination.
     * <p/>
     * The full name of the JMS destination including the prefix should be provided - i.e. queue://myQueue
     * or topic://myTopic.  If the destination type prefix is not included in the destination name, a prefix
     * of "queue://" is assumed.
     *
     * @param destinationName the full name of the JMS Destination
     * @return the number of messages in the JMS Destination
     */
    public long getMessageCount(String destinationName) {
        if (null == brokerService) {
            throw new IllegalStateException("BrokerService has not yet been created - was before() called?");
        }

        // TODO: Figure out how to do this for Topics
        Destination destination = getDestination(destinationName);
        if (destination == null) {
            throw new RuntimeException("Failed to find destination: " + destinationName);
        }

        // return destination.getMessageStore().getMessageCount();
        return destination.getDestinationStatistics().getMessages().getCount();
    }

    /**
     * Get the ActiveMQ destination
     * <p/>
     * The full name of the JMS destination including the prefix should be provided - i.e. queue://myQueue
     * or topic://myTopic.  If the destination type prefix is not included in the destination name, a prefix
     * of "queue://" is assumed.
     *
     * @param destinationName the full name of the JMS Destination
     * @return the ActiveMQ destination, null if not found
     */
    public Destination getDestination(String destinationName) {
        if (null == brokerService) {
            throw new IllegalStateException("BrokerService has not yet been created - was before() called?");
        }

        Destination destination = null;
        try {
            destination = brokerService.getDestination(ActiveMQDestination.createDestination(destinationName, QUEUE_TYPE));
        } catch (RuntimeException runtimeEx) {
            throw runtimeEx;
        } catch (Exception ex) {
            throw new EmbeddedActiveMQBrokerException("Unexpected exception getting destination from broker", ex);
        }

        return destination;
    }

    private PolicyEntry getDefaultPolicyEntry() {
        PolicyMap destinationPolicy = brokerService.getDestinationPolicy();
        if (null == destinationPolicy) {
            destinationPolicy = new PolicyMap();
            brokerService.setDestinationPolicy(destinationPolicy);
        }

        PolicyEntry defaultEntry = destinationPolicy.getDefaultEntry();
        if (null == defaultEntry) {
            defaultEntry = new PolicyEntry();
            destinationPolicy.setDefaultEntry(defaultEntry);
        }

        return defaultEntry;
    }

    public BytesMessage createBytesMessage() {
        return internalClient.createBytesMessage();
    }

    public TextMessage createTextMessage() {
        return internalClient.createTextMessage();
    }

    public MapMessage createMapMessage() {
        return internalClient.createMapMessage();
    }

    public ObjectMessage createObjectMessage() {
        return internalClient.createObjectMessage();
    }

    public StreamMessage createStreamMessage() {
        return internalClient.createStreamMessage();
    }

    public BytesMessage createMessage(byte[] body) {
        return this.createMessage(body, null);
    }

    public TextMessage createMessage(String body) {
        return this.createMessage(body, null);
    }

    public MapMessage createMessage(Map<String, Object> body) {
        return this.createMessage(body, null);
    }

    public ObjectMessage createMessage(Serializable body) {
        return this.createMessage(body, null);
    }

    public BytesMessage createMessage(byte[] body, Map<String, Object> properties) {
        BytesMessage message = this.createBytesMessage();
        if (body != null) {
            try {
                message.writeBytes(body);
            } catch (JMSException jmsEx) {
                throw new EmbeddedActiveMQBrokerException(String.format("Failed to set body {%s} on BytesMessage", new String(body)), jmsEx);
            }
        }

        setMessageProperties(message, properties);

        return message;
    }

    public TextMessage createMessage(String body, Map<String, Object> properties) {
        TextMessage message = this.createTextMessage();
        if (body != null) {
            try {
                message.setText(body);
            } catch (JMSException jmsEx) {
                throw new EmbeddedActiveMQBrokerException(String.format("Failed to set body {%s} on TextMessage", body), jmsEx);
            }
        }

        setMessageProperties(message, properties);

        return message;
    }

    public MapMessage createMessage(Map<String, Object> body, Map<String, Object> properties) {
        MapMessage message = this.createMapMessage();

        if (body != null) {
            for (Map.Entry<String, Object> entry : body.entrySet()) {
                try {
                    message.setObject(entry.getKey(), entry.getValue());
                } catch (JMSException jmsEx) {
                    throw new EmbeddedActiveMQBrokerException(String.format("Failed to set body entry {%s = %s} on MapMessage", entry.getKey(), entry.getValue().toString()), jmsEx);
                }
            }
        }

        setMessageProperties(message, properties);

        return message;
    }

    public ObjectMessage createMessage(Serializable body, Map<String, Object> properties) {
        ObjectMessage message = this.createObjectMessage();

        if (body != null) {
            try {
                message.setObject(body);
            } catch (JMSException jmsEx) {
                throw new EmbeddedActiveMQBrokerException(String.format("Failed to set body {%s} on ObjectMessage", body.toString()), jmsEx);
            }
        }

        setMessageProperties(message, properties);

        return message;
    }

    public void pushMessage(String destinationName, Message message) {
        if (destinationName == null) {
            throw new IllegalArgumentException("pushMessage failure - destination name is required");
        } else if (message == null) {
            throw new IllegalArgumentException("pushMessage failure - a Message is required");
        }
        ActiveMQDestination destination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);

        internalClient.pushMessage(destination, message);
    }

    public BytesMessage pushMessage(String destinationName, byte[] body) {
        BytesMessage message = createMessage(body, null);
        pushMessage(destinationName, message);
        return message;
    }

    public TextMessage pushMessage(String destinationName, String body) {
        TextMessage message = createMessage(body, null);
        pushMessage(destinationName, message);
        return message;
    }

    public MapMessage pushMessage(String destinationName, Map<String, Object> body) {
        MapMessage message = createMessage(body, null);
        pushMessage(destinationName, message);
        return message;
    }

    public ObjectMessage pushMessage(String destinationName, Serializable body) {
        ObjectMessage message = createMessage(body, null);
        pushMessage(destinationName, message);
        return message;
    }

    public BytesMessage pushMessageWithProperties(String destinationName, byte[] body, Map<String, Object> properties) {
        BytesMessage message = createMessage(body, properties);
        pushMessage(destinationName, message);
        return message;
    }

    public TextMessage pushMessageWithProperties(String destinationName, String body, Map<String, Object> properties) {
        TextMessage message = createMessage(body, properties);
        pushMessage(destinationName, message);
        return message;
    }

    public MapMessage pushMessageWithProperties(String destinationName, Map<String, Object> body, Map<String, Object> properties) {
        MapMessage message = createMessage(body, properties);
        pushMessage(destinationName, message);
        return message;
    }

    public ObjectMessage pushMessageWithProperties(String destinationName, Serializable body, Map<String, Object> properties) {
        ObjectMessage message = createMessage(body, properties);
        pushMessage(destinationName, message);
        return message;
    }


    public Message peekMessage(String destinationName) {
        if (null == brokerService) {
            throw new NullPointerException("peekMessage failure  - BrokerService is null");
        }

        if (destinationName == null) {
            throw new IllegalArgumentException("peekMessage failure - destination name is required");
        }

        ActiveMQDestination destination = ActiveMQDestination.createDestination(destinationName, ActiveMQDestination.QUEUE_TYPE);
        Destination brokerDestination = null;

        try {
            brokerDestination = brokerService.getDestination(destination);
        } catch (Exception ex) {
            throw new EmbeddedActiveMQBrokerException("peekMessage failure - unexpected exception getting destination from BrokerService", ex);
        }

        if (brokerDestination == null) {
            throw new IllegalStateException(String.format("peekMessage failure - destination %s not found in broker %s", destination.toString(), brokerService.getBrokerName()));
        }

        org.apache.activemq.command.Message[] messages = brokerDestination.browse();
        if (messages != null && messages.length > 0) {
            return (Message) messages[0];
        }

        return null;
    }

    public BytesMessage peekBytesMessage(String destinationName) {
        return (BytesMessage) peekMessage(destinationName);
    }

    public TextMessage peekTextMessage(String destinationName) {
        return (TextMessage) peekMessage(destinationName);
    }

    public MapMessage peekMapMessage(String destinationName) {
        return (MapMessage) peekMessage(destinationName);
    }

    public ObjectMessage peekObjectMessage(String destinationName) {
        return (ObjectMessage) peekMessage(destinationName);
    }

    public StreamMessage peekStreamMessage(String destinationName) {
        return (StreamMessage) peekMessage(destinationName);
    }

    public static class EmbeddedActiveMQBrokerException extends RuntimeException {
        public EmbeddedActiveMQBrokerException(String message) {
            super(message);
        }

        public EmbeddedActiveMQBrokerException(String message, Exception cause) {
            super(message, cause);
        }
    }

    private class InternalClient {
        ActiveMQConnectionFactory connectionFactory;
        Connection connection;
        Session session;
        MessageProducer producer;

        public InternalClient() {
        }

        void start() {
            connectionFactory = createConnectionFactory();
            try {
                connection = connectionFactory.createConnection();
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                producer = session.createProducer(null);
                connection.start();
            } catch (JMSException jmsEx) {
                throw new EmbeddedActiveMQBrokerException("Internal Client creation failure", jmsEx);
            }
        }

        void stop() {
            if (null != connection) {
                try {
                    connection.close();
                } catch (JMSException jmsEx) {
                    log.warn("JMSException encounter closing InternalClient connection - ignoring", jmsEx);
                }
            }
        }

        public BytesMessage createBytesMessage() {
            checkSession();

            try {
                return session.createBytesMessage();
            } catch (JMSException jmsEx) {
                throw new EmbeddedActiveMQBrokerException("Failed to create BytesMessage", jmsEx);
            }
        }

        public TextMessage createTextMessage() {
            checkSession();

            try {
                return session.createTextMessage();
            } catch (JMSException jmsEx) {
                throw new EmbeddedActiveMQBrokerException("Failed to create TextMessage", jmsEx);
            }
        }

        public MapMessage createMapMessage() {
            checkSession();

            try {
                return session.createMapMessage();
            } catch (JMSException jmsEx) {
                throw new EmbeddedActiveMQBrokerException("Failed to create MapMessage", jmsEx);
            }
        }

        public ObjectMessage createObjectMessage() {
            checkSession();

            try {
                return session.createObjectMessage();
            } catch (JMSException jmsEx) {
                throw new EmbeddedActiveMQBrokerException("Failed to create ObjectMessage", jmsEx);
            }
        }

        public StreamMessage createStreamMessage() {
            checkSession();
            try {
                return session.createStreamMessage();
            } catch (JMSException jmsEx) {
                throw new EmbeddedActiveMQBrokerException("Failed to create StreamMessage", jmsEx);
            }
        }

        public void pushMessage(ActiveMQDestination destination, Message message) {
            if (producer == null) {
                throw new IllegalStateException("JMS MessageProducer is null - has the InternalClient been started?");
            }

            try {
                producer.send(destination, message);
            } catch (JMSException jmsEx) {
                throw new EmbeddedActiveMQBrokerException(String.format("Failed to push %s to %s", message.getClass().getSimpleName(), destination.toString()), jmsEx);
            }
        }

        void checkSession() {
            if (session == null) {
                throw new IllegalStateException("JMS Session is null - has the InternalClient been started?");
            }
        }
    }
}
