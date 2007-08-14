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
package org.apache.activemq.broker.jmx;

import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.ObjectName;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.RemoveSubscriptionInfo;
//import org.apache.log4j.LogManager;
//import org.apache.log4j.PropertyConfigurator;

public class BrokerView implements BrokerViewMBean {

    final ManagedRegionBroker broker;
    private final BrokerService brokerService;
    private final AtomicInteger sessionIdCounter = new AtomicInteger(0);

    public BrokerView(BrokerService brokerService, ManagedRegionBroker managedBroker) throws Exception {
        this.brokerService = brokerService;
        this.broker = managedBroker;
    }

    public ManagedRegionBroker getBroker() {
        return broker;
    }

    public String getBrokerId() {
        return broker.getBrokerId().toString();
    }

    public void gc() throws Exception {
        brokerService.getBroker().gc();
    }

    public void start() throws Exception {
        brokerService.start();
    }

    public void stop() throws Exception {
        brokerService.stop();
    }

    public long getTotalEnqueueCount() {
        return broker.getDestinationStatistics().getEnqueues().getCount();
    }

    public long getTotalDequeueCount() {
        return broker.getDestinationStatistics().getDequeues().getCount();
    }

    public long getTotalConsumerCount() {
        return broker.getDestinationStatistics().getConsumers().getCount();
    }

    public long getTotalMessageCount() {
        return broker.getDestinationStatistics().getMessages().getCount();
    }

    public long getTotalMessagesCached() {
        return broker.getDestinationStatistics().getMessagesCached().getCount();
    }

    public int getMemoryPercentageUsed() {
        return brokerService.getMemoryManager().getPercentUsage();
    }

    public long getMemoryLimit() {
        return brokerService.getMemoryManager().getLimit();
    }

    public void setMemoryLimit(long limit) {
        brokerService.getMemoryManager().setLimit(limit);
    }

    public void resetStatistics() {
        broker.getDestinationStatistics().reset();
    }

    public void enableStatistics() {
        broker.getDestinationStatistics().setEnabled(true);
    }

    public void disableStatistics() {
        broker.getDestinationStatistics().setEnabled(false);
    }

    public boolean isStatisticsEnabled() {
        return broker.getDestinationStatistics().isEnabled();
    }

    public void terminateJVM(int exitCode) {
        System.exit(exitCode);
    }

    public ObjectName[] getTopics() {
        return broker.getTopics();
    }

    public ObjectName[] getQueues() {
        return broker.getQueues();
    }

    public ObjectName[] getTemporaryTopics() {
        return broker.getTemporaryTopics();
    }

    public ObjectName[] getTemporaryQueues() {
        return broker.getTemporaryQueues();
    }

    public ObjectName[] getTopicSubscribers() {
        return broker.getTemporaryTopicSubscribers();
    }

    public ObjectName[] getDurableTopicSubscribers() {
        return broker.getDurableTopicSubscribers();
    }

    public ObjectName[] getQueueSubscribers() {
        return broker.getQueueSubscribers();
    }

    public ObjectName[] getTemporaryTopicSubscribers() {
        return broker.getTemporaryTopicSubscribers();
    }

    public ObjectName[] getTemporaryQueueSubscribers() {
        return broker.getTemporaryQueueSubscribers();
    }

    public ObjectName[] getInactiveDurableTopicSubscribers() {
        return broker.getInactiveDurableTopicSubscribers();
    }

    public void addTopic(String name) throws Exception {
        broker.addDestination(getConnectionContext(broker.getContextBroker()), new ActiveMQTopic(name));
    }

    public void addQueue(String name) throws Exception {
        broker.addDestination(getConnectionContext(broker.getContextBroker()), new ActiveMQQueue(name));
    }

    public void removeTopic(String name) throws Exception {
        broker.removeDestination(getConnectionContext(broker.getContextBroker()), new ActiveMQTopic(name),
                                 1000);
    }

    public void removeQueue(String name) throws Exception {
        broker.removeDestination(getConnectionContext(broker.getContextBroker()), new ActiveMQQueue(name),
                                 1000);
    }

    public ObjectName createDurableSubscriber(String clientId, String subscriberName, String topicName,
                                              String selector) throws Exception {
        ConnectionContext context = new ConnectionContext();
        context.setBroker(broker);
        context.setClientId(clientId);
        ConsumerInfo info = new ConsumerInfo();
        ConsumerId consumerId = new ConsumerId();
        consumerId.setConnectionId(clientId);
        consumerId.setSessionId(sessionIdCounter.incrementAndGet());
        consumerId.setValue(0);
        info.setConsumerId(consumerId);
        info.setDestination(new ActiveMQTopic(topicName));
        info.setSubscriptionName(subscriberName);
        info.setSelector(selector);
        Subscription subscription = broker.addConsumer(context, info);
        broker.removeConsumer(context, info);
        if (subscription != null) {
            return subscription.getObjectName();
        }
        return null;
    }

    public void destroyDurableSubscriber(String clientId, String subscriberName) throws Exception {
        RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
        info.setClientId(clientId);
        info.setSubscriptionName(subscriberName);
        ConnectionContext context = new ConnectionContext();
        context.setBroker(broker);
        context.setClientId(clientId);
        broker.removeSubscription(context, info);
    }

    /**
     * Returns the broker's administration connection context used for
     * configuring the broker at startup
     */
    public static ConnectionContext getConnectionContext(Broker broker) {
        ConnectionContext adminConnectionContext = broker.getAdminConnectionContext();
        if (adminConnectionContext == null) {
            adminConnectionContext = createAdminConnectionContext(broker);
            broker.setAdminConnectionContext(adminConnectionContext);
        }
        return adminConnectionContext;
    }

    /**
     * Factory method to create the new administration connection context
     * object. Note this method is here rather than inside a default broker
     * implementation to ensure that the broker reference inside it is the outer
     * most interceptor
     */
    protected static ConnectionContext createAdminConnectionContext(Broker broker) {
        ConnectionContext context = new ConnectionContext();
        context.setBroker(broker);
        return context;
    }
    
    //  doc comment inherited from BrokerViewMBean
    public void reloadLog4jProperties() throws Exception {
        /*
        LogManager.resetConfiguration();
        ClassLoader cl = this.getClass().getClassLoader();
        URL log4jprops = cl.getResource("log4j.properties");
        if (log4jprops != null) {
            PropertyConfigurator.configure(log4jprops);
        }
        */
    }
    

}
