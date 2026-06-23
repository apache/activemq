/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.web.stubs;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectionViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.jmx.JobSchedulerViewMBean;
import org.apache.activemq.broker.jmx.NetworkBridgeViewMBean;
import org.apache.activemq.broker.jmx.NetworkConnectorViewMBean;
import org.apache.activemq.broker.jmx.ProducerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.web.BrokerFacade;
import org.apache.activemq.web.JobFacade;

import java.util.Collection;
import java.util.List;

/**
 * Stub implementation of the BrokerFacade interface for testing purposes. All methods return empty responses or equivalents.
 */
public class StubBrokerFacade implements BrokerFacade {

    final BrokerViewMBean brokerAdmin;

    public StubBrokerFacade() {
        brokerAdmin = null;
    }

    public StubBrokerFacade(BrokerViewMBean brokerAdmin) {
        this.brokerAdmin = brokerAdmin;
    }

    @Override
    public String getBrokerName() throws Exception {
        return "";
    }

    @Override
    public BrokerViewMBean getBrokerAdmin() throws Exception {
        return brokerAdmin;
    }

    @Override
    public Collection<QueueViewMBean> getQueues() throws Exception {
        return List.of();
    }

    @Override
    public Collection<TopicViewMBean> getTopics() throws Exception {
        return List.of();
    }

    @Override
    public Collection<SubscriptionViewMBean> getQueueConsumers(String queueName) throws Exception {
        return List.of();
    }

    @Override
    public Collection<ProducerViewMBean> getQueueProducers(String queueName) throws Exception {
        return List.of();
    }

    @Override
    public Collection<ProducerViewMBean> getTopicProducers(String queueName) throws Exception {
        return List.of();
    }

    @Override
    public Collection<SubscriptionViewMBean> getTopicSubscribers(String topicName) throws Exception {
        return List.of();
    }

    @Override
    public Collection<SubscriptionViewMBean> getNonDurableTopicSubscribers() throws Exception {
        return List.of();
    }

    @Override
    public Collection<DurableSubscriptionViewMBean> getDurableTopicSubscribers() throws Exception {
        return List.of();
    }

    @Override
    public Collection<DurableSubscriptionViewMBean> getInactiveDurableTopicSubscribers() throws Exception {
        return List.of();
    }

    @Override
    public Collection<ConnectorViewMBean> getConnectors() throws Exception {
        return List.of();
    }

    @Override
    public ConnectorViewMBean getConnector(String name) throws Exception {
        return null;
    }

    @Override
    public Collection<ConnectionViewMBean> getConnections() throws Exception {
        return List.of();
    }

    @Override
    public Collection<ConnectionViewMBean> getConnections(String connectorName) throws Exception {
        return List.of();
    }

    @Override
    public ConnectionViewMBean getConnection(String connectionName) throws Exception {
        return null;
    }

    @Override
    public Collection<SubscriptionViewMBean> getConsumersOnConnection(String connectionName) throws Exception {
        return List.of();
    }

    @Override
    public Collection<NetworkConnectorViewMBean> getNetworkConnectors() throws Exception {
        return List.of();
    }

    @Override
    public Collection<NetworkBridgeViewMBean> getNetworkBridges() throws Exception {
        return List.of();
    }

    @Override
    public void purgeQueue(ActiveMQDestination destination) throws Exception {

    }

    @Override
    public QueueViewMBean getQueue(String name) throws Exception {
        return null;
    }

    @Override
    public TopicViewMBean getTopic(String name) throws Exception {
        return null;
    }

    @Override
    public JobSchedulerViewMBean getJobScheduler() throws Exception {
        return null;
    }

    @Override
    public Collection<JobFacade> getScheduledJobs() throws Exception {
        return List.of();
    }

    @Override
    public boolean isJobSchedulerStarted() {
        return false;
    }
}
