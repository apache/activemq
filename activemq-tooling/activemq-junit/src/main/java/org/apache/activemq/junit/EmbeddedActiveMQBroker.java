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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
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

    /**
     * Create an embedded ActiveMQ broker using defaults
     *
     * The defaults are:
     *  - the broker name is 'embedded-broker'
     *  - JMX is disabled
     *  - Persistence is disabled
     *
     */
    public EmbeddedActiveMQBroker() {
        brokerService = new BrokerService();
        brokerService.setUseJmx(false);
        brokerService.setUseShutdownHook(false);
        brokerService.setPersistent(false);
        brokerService.setBrokerName("embedded-broker");
    }

    /**
     * Create an embedded ActiveMQ broker using a configuration URI
     */
    public EmbeddedActiveMQBroker(String configurationURI ) {
        try {
            brokerService = BrokerFactory.createBroker(configurationURI);
        } catch (Exception ex) {
            throw new RuntimeException("Exception encountered creating embedded ActiveMQ broker from configuration URI: " + configurationURI, ex);
        }
    }

    /**
     * Create an embedded ActiveMQ broker using a configuration URI
     */
    public EmbeddedActiveMQBroker(URI configurationURI ) {
        try {
            brokerService = BrokerFactory.createBroker(configurationURI);
        } catch (Exception ex) {
            throw new RuntimeException("Exception encountered creating embedded ActiveMQ broker from configuration URI: " + configurationURI, ex);
        }
    }

    /**
     * Customize the configuration of the embedded ActiveMQ broker
     *
     * This method is called before the embedded ActiveMQ broker is started, and can
     * be overridden to this method to customize the broker configuration.
     */
    protected void configure() {}

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
        connectionFactory.setBrokerURL(brokerService.getVmConnectorURI().toString());
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
     * Get the VM URL for the embedded ActiveMQ Broker
     * <p/>
     * NOTE:  The option is precreate=false option is appended to the URL to avoid the automatic creation of brokers
     * and the resulting duplicate broker errors
     *
     * @return the VM URL for the embedded broker
     */
    public String getVmURL() {
        return String.format("failover:(%s?create=false)", brokerService.getVmConnectorURI().toString());
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
     * The full name of the JMS destination including the prefix should be provided - i.e. queue:myQueue
     * or topic:myTopic.  If the destination type prefix is not included in the destination name, a prefix
     * of "queue:" is assumed.
     *
     * @param fullDestinationName the full name of the JMS Destination
     * @return the number of messages in the JMS Destination
     */
    public int getMessageCount(String fullDestinationName) throws Exception {
        final int QUEUE_TYPE = 1;
        final int TOPIC_TYPE = 2;

        if (null == brokerService) {
            throw new IllegalStateException("BrokerService has not yet been created - was before() called?");
        }

        int destinationType = QUEUE_TYPE;
        String destinationName = fullDestinationName;

        if (fullDestinationName.startsWith("queue:")) {
            destinationName = fullDestinationName.substring(fullDestinationName.indexOf(':') + 1);
        } else if (fullDestinationName.startsWith("topic:")) {
            destinationType = TOPIC_TYPE;
            destinationName = fullDestinationName.substring(fullDestinationName.indexOf(':') + 1);
        }

        int messageCount = -1;
        boolean foundDestination = false;
        for (Destination destination : brokerService.getBroker().getDestinationMap().values()) {
            String tmpName = destination.getName();
            if (tmpName.equalsIgnoreCase(destinationName)) {
                switch (destinationType) {
                    case QUEUE_TYPE:
                        if (destination instanceof Queue) {
                            messageCount = destination.getMessageStore().getMessageCount();
                            foundDestination = true;
                        }
                        break;
                    case TOPIC_TYPE:
                        if (destination instanceof Topic) {
                            messageCount = destination.getMessageStore().getMessageCount();
                            foundDestination = true;
                        }
                        break;
                    default:
                        // Should never see this
                        log.error("Type didn't match: {}", destination.getClass().getName());
                }
            }
            if (foundDestination) {
                break;
            }
        }

        if (!foundDestination) {
            log.warn("Didn't find destination {} in broker {}", fullDestinationName, getBrokerName());
        }

        return messageCount;
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
}
