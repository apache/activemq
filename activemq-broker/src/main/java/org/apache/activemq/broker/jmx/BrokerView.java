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

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

import org.apache.activemq.ActiveMQConnectionMetaData;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.*;
import org.apache.activemq.network.NetworkConnector;
import org.apache.activemq.util.BrokerSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokerView implements BrokerViewMBean {

    private static final Logger LOG = LoggerFactory.getLogger(BrokerView.class);

    ManagedRegionBroker broker;

    private final BrokerService brokerService;
    private final AtomicInteger sessionIdCounter = new AtomicInteger(0);
    private ObjectName jmsJobScheduler;

    public BrokerView(BrokerService brokerService, ManagedRegionBroker managedBroker) throws Exception {
        this.brokerService = brokerService;
        this.broker = managedBroker;
    }

    public ManagedRegionBroker getBroker() {
        return broker;
    }

    public void setBroker(ManagedRegionBroker broker) {
        this.broker = broker;
    }

    @Override
    public String getBrokerId() {
        return safeGetBroker().getBrokerId().toString();
    }

    @Override
    public String getBrokerName() {
        return safeGetBroker().getBrokerName();
    }

    @Override
    public String getBrokerVersion() {
        return ActiveMQConnectionMetaData.PROVIDER_VERSION;
    }

    @Override
    public String getUptime() {
        return brokerService.getUptime();
    }

    @Override
    public long getUptimeMillis() {
        return brokerService.getUptimeMillis();
    }

    @Override
    public int getCurrentConnectionsCount() {
        return brokerService.getCurrentConnections();
    }

    @Override
    public long getTotalConnectionsCount() {
        return brokerService.getTotalConnections();
    }

    @Override
    public void gc() throws Exception {
        brokerService.getBroker().gc();
        try {
            brokerService.getPersistenceAdapter().checkpoint(true);
        } catch (IOException e) {
            LOG.error("Failed to checkpoint persistence adapter on gc request", e);
        }
    }

    @Override
    public void start() throws Exception {
        brokerService.start();
    }

    @Override
    public void stop() throws Exception {
        brokerService.stop();
    }

    @Override
    public void restart() throws Exception {
        if (brokerService.isRestartAllowed()) {
            brokerService.requestRestart();
            brokerService.stop();
        } else {
            throw new Exception("Restart is not allowed");
        }
    }

    @Override
    public void stopGracefully(String connectorName, String queueName, long timeout, long pollInterval) throws Exception {
        brokerService.stopGracefully(connectorName, queueName, timeout, pollInterval);
    }

    @Override
    public long getTotalEnqueueCount() {
        return safeGetBroker().getDestinationStatistics().getEnqueues().getCount();
    }

    @Override
    public long getTotalDequeueCount() {
        return safeGetBroker().getDestinationStatistics().getDequeues().getCount();
    }

    @Override
    public long getTotalConsumerCount() {
        return safeGetBroker().getDestinationStatistics().getConsumers().getCount();
    }

    @Override
    public long getTotalProducerCount() {
        return safeGetBroker().getDestinationStatistics().getProducers().getCount();
    }

    @Override
    public long getTotalMessageCount() {
        return safeGetBroker().getDestinationStatistics().getMessages().getCount();
    }

    /**
     * @return the average size of a message (bytes)
     */
    @Override
    public long getAverageMessageSize() {
        // we are okay with the size without decimals so cast to long
        return (long) safeGetBroker().getDestinationStatistics().getMessageSize().getAverageSize();
    }

    /**
     * @return the max size of a message (bytes)
     */
    @Override
    public long getMaxMessageSize() {
        return safeGetBroker().getDestinationStatistics().getMessageSize().getMaxSize();
    }

    /**
     * @return the min size of a message (bytes)
     */
    @Override
    public long getMinMessageSize() {
        return safeGetBroker().getDestinationStatistics().getMessageSize().getMinSize();
    }

    public long getTotalMessagesCached() {
        return safeGetBroker().getDestinationStatistics().getMessagesCached().getCount();
    }

    @Override
    public int getMemoryPercentUsage() {
        return brokerService.getSystemUsage().getMemoryUsage().getPercentUsage();
    }

    @Override
    public long getMemoryLimit() {
        return brokerService.getSystemUsage().getMemoryUsage().getLimit();
    }

    @Override
    public void setMemoryLimit(long limit) {
        brokerService.getSystemUsage().getMemoryUsage().setLimit(limit);
    }

    @Override
    public long getStoreLimit() {
        return brokerService.getSystemUsage().getStoreUsage().getLimit();
    }

    @Override
    public int getStorePercentUsage() {
        return brokerService.getSystemUsage().getStoreUsage().getPercentUsage();
    }

    @Override
    public long getTempLimit() {
        return brokerService.getSystemUsage().getTempUsage().getLimit();
    }

    @Override
    public int getTempPercentUsage() {
        return brokerService.getSystemUsage().getTempUsage().getPercentUsage();
    }

    @Override
    public long getJobSchedulerStoreLimit() {
        return brokerService.getSystemUsage().getJobSchedulerUsage().getLimit();
    }

    @Override
    public int getJobSchedulerStorePercentUsage() {
        return brokerService.getSystemUsage().getJobSchedulerUsage().getPercentUsage();
    }

    @Override
    public void setStoreLimit(long limit) {
        brokerService.getSystemUsage().getStoreUsage().setLimit(limit);
    }

    @Override
    public void setTempLimit(long limit) {
        brokerService.getSystemUsage().getTempUsage().setLimit(limit);
    }

    @Override
    public void setJobSchedulerStoreLimit(long limit) {
        brokerService.getSystemUsage().getJobSchedulerUsage().setLimit(limit);
    }

    @Override
    public void resetStatistics() {
        safeGetBroker().getDestinationStatistics().reset();
    }

    @Override
    public void enableStatistics() {
        safeGetBroker().getDestinationStatistics().setEnabled(true);
    }

    @Override
    public void disableStatistics() {
        safeGetBroker().getDestinationStatistics().setEnabled(false);
    }

    @Override
    public boolean isStatisticsEnabled() {
        return safeGetBroker().getDestinationStatistics().isEnabled();
    }

    @Override
    public boolean isPersistent() {
        return brokerService.isPersistent();
    }

    @Override
    public void terminateJVM(int exitCode) {
        System.exit(exitCode);
    }

    @Override
    public ObjectName[] getTopics() {
        return safeGetBroker().getTopicsNonSuppressed();
    }

    @Override
    public ObjectName[] getQueues() {
        return safeGetBroker().getQueuesNonSuppressed();
    }

    @Override
    public String queryQueues(String filter, int page, int pageSize) throws IOException {
        return DestinationsViewFilter.create(filter)
                .setDestinations(safeGetBroker().getQueueViews())
                .filter(page, pageSize);
    }

    @Override
    public String queryTopics(String filter, int page, int pageSize) throws IOException {
        return DestinationsViewFilter.create(filter)
                .setDestinations(safeGetBroker().getTopicViews())
                .filter(page, pageSize);
    }

    public CompositeData[] browseQueue(String queueName) throws OpenDataException, MalformedObjectNameException {
       return safeGetBroker().getQueueView(queueName).browse();
    }

    @Override
    public ObjectName[] getTemporaryTopics() {
        return safeGetBroker().getTemporaryTopicsNonSuppressed();
    }

    @Override
    public ObjectName[] getTemporaryQueues() {
        return safeGetBroker().getTemporaryQueuesNonSuppressed();
    }

    @Override
    public ObjectName[] getTopicSubscribers() {
        return safeGetBroker().getTopicSubscribersNonSuppressed();
    }

    @Override
    public ObjectName[] getDurableTopicSubscribers() {
        return safeGetBroker().getDurableTopicSubscribersNonSuppressed();
    }

    @Override
    public ObjectName[] getQueueSubscribers() {
        return safeGetBroker().getQueueSubscribersNonSuppressed();
    }

    @Override
    public ObjectName[] getTemporaryTopicSubscribers() {
        return safeGetBroker().getTemporaryTopicSubscribersNonSuppressed();
    }

    @Override
    public ObjectName[] getTemporaryQueueSubscribers() {
        return safeGetBroker().getTemporaryQueueSubscribersNonSuppressed();
    }

    @Override
    public ObjectName[] getInactiveDurableTopicSubscribers() {
        return safeGetBroker().getInactiveDurableTopicSubscribersNonSuppressed();
    }

    @Override
    public ObjectName[] getTopicProducers() {
        return safeGetBroker().getTopicProducersNonSuppressed();
    }

    @Override
    public ObjectName[] getQueueProducers() {
        return safeGetBroker().getQueueProducersNonSuppressed();
    }

    @Override
    public ObjectName[] getTemporaryTopicProducers() {
        return safeGetBroker().getTemporaryTopicProducersNonSuppressed();
    }

    @Override
    public ObjectName[] getTemporaryQueueProducers() {
        return safeGetBroker().getTemporaryQueueProducersNonSuppressed();
    }

    @Override
    public ObjectName[] getDynamicDestinationProducers() {
        return safeGetBroker().getDynamicDestinationProducersNonSuppressed();
    }

    @Override
    public String addConnector(String discoveryAddress) throws Exception {
        TransportConnector connector = brokerService.addConnector(discoveryAddress);
        if (connector == null) {
            throw new NoSuchElementException("Not connector matched the given name: " + discoveryAddress);
        }
        brokerService.startTransportConnector(connector);
        return connector.getName();
    }

    @Override
    public String addNetworkConnector(String discoveryAddress) throws Exception {
        NetworkConnector connector = brokerService.addNetworkConnector(discoveryAddress);
        if (connector == null) {
            throw new NoSuchElementException("Not connector matched the given name: " + discoveryAddress);
        }
        brokerService.registerNetworkConnectorMBean(connector);
        connector.start();
        return connector.getName();
    }

    @Override
    public boolean removeConnector(String connectorName) throws Exception {
        TransportConnector connector = brokerService.getConnectorByName(connectorName);
        if (connector == null) {
            throw new NoSuchElementException("Not connector matched the given name: " + connectorName);
        }
        connector.stop();
        return brokerService.removeConnector(connector);
    }

    @Override
    public boolean removeNetworkConnector(String connectorName) throws Exception {
        NetworkConnector connector = brokerService.getNetworkConnectorByName(connectorName);
        if (connector == null) {
            throw new NoSuchElementException("Not connector matched the given name: " + connectorName);
        }
        connector.stop();
        return brokerService.removeNetworkConnector(connector);
    }

    @Override
    public void addTopic(String name) throws Exception {
        safeGetBroker().getContextBroker()
            .addDestination(BrokerSupport.getConnectionContext(safeGetBroker().getContextBroker()), new ActiveMQTopic(name), true);
    }

    @Override
    public void addQueue(String name) throws Exception {
        safeGetBroker().getContextBroker()
            .addDestination(BrokerSupport.getConnectionContext(safeGetBroker().getContextBroker()), new ActiveMQQueue(name), true);
    }

    @Override
    public void removeTopic(String name) throws Exception {
        safeGetBroker().getContextBroker().removeDestination(BrokerSupport.getConnectionContext(safeGetBroker().getContextBroker()), new ActiveMQTopic(name), 1000);
    }

    @Override
    public void removeQueue(String name) throws Exception {
        safeGetBroker().getContextBroker().removeDestination(BrokerSupport.getConnectionContext(safeGetBroker().getContextBroker()), new ActiveMQQueue(name), 1000);
    }

    @Override
    public ObjectName createDurableSubscriber(String clientId, String subscriberName, String topicName, String selector) throws Exception {
        ConnectionContext context = getConnectionContext();
        context.setBroker(safeGetBroker());
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
        Subscription subscription = safeGetBroker().addConsumer(context, info);
        safeGetBroker().removeConsumer(context, info);
        if (subscription != null) {
            return subscription.getObjectName();
        }
        return null;
    }

    @Override
    public void destroyDurableSubscriber(String clientId, String subscriberName) throws Exception {
        RemoveSubscriptionInfo info = new RemoveSubscriptionInfo();
        info.setClientId(clientId);
        info.setSubscriptionName(subscriberName);
        ConnectionContext context = getConnectionContext();
        context.setBroker(safeGetBroker());
        context.setClientId(clientId);
        brokerService.getBroker().removeSubscription(context, info);
    }

    @Override
    public void reloadLog4jProperties() throws Throwable {
        Log4JConfigView.doReloadLog4jProperties();
    }

    @Override
    public Map<String, String> getTransportConnectors() {
        Map<String, String> answer = new HashMap<String, String>();
        try {
            for (TransportConnector connector : brokerService.getTransportConnectors()) {
                answer.put(connector.getName(), connector.getConnectUri().toString());
            }
        } catch (Exception e) {
            LOG.debug("Failed to read URI to build transport connectors map", e);
        }
        return answer;
    }

    @Override
    public String getTransportConnectorByType(String type) {
        return brokerService.getTransportConnectorURIsAsMap().get(type);
    }

    @Override
    public String getVMURL() {
        URI answer = brokerService.getVmConnectorURI();
        return answer != null ? answer.toString() : "";
    }

    @Override
    public String getDataDirectory() {
        File file = brokerService.getDataDirectoryFile();
        try {
            return file != null ? file.getCanonicalPath() : "";
        } catch (IOException e) {
            return "";
        }
    }

    @Override
    public ObjectName getJMSJobScheduler() {
        return this.jmsJobScheduler;
    }

    public void setJMSJobScheduler(ObjectName name) {
        this.jmsJobScheduler = name;
    }

    @Override
    public boolean isSlave() {
        return brokerService.isSlave();
    }

    private ManagedRegionBroker safeGetBroker() {
        if (broker == null) {
            throw new IllegalStateException("Broker is not yet started.");
        }

        return broker;
    }

    private ConnectionContext getConnectionContext() {
        ConnectionContext context;
        if (broker == null) {
            context = new ConnectionContext();
        } else {
            ConnectionContext sharedContext = BrokerSupport.getConnectionContext(broker.getContextBroker());
            // Make a local copy of the sharedContext. We do this because we do
            // not want to set a clientId on the
            // global sharedContext. Taking a copy of the sharedContext is a
            // good way to make sure that we are not
            // messing up the shared context
            context = sharedContext.copy();
        }

        return context;
    }
}
