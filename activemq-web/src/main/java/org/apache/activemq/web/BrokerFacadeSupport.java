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
package org.apache.activemq.web;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import javax.management.InstanceNotFoundException;
import javax.management.ObjectName;
import javax.management.QueryExp;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.apache.activemq.broker.jmx.BrokerViewMBean;
import org.apache.activemq.broker.jmx.ConnectionViewMBean;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.broker.jmx.DestinationViewMBean;
import org.apache.activemq.broker.jmx.DurableSubscriptionViewMBean;
import org.apache.activemq.broker.jmx.JobSchedulerViewMBean;
import org.apache.activemq.broker.jmx.ManagementContext;
import org.apache.activemq.broker.jmx.NetworkBridgeViewMBean;
import org.apache.activemq.broker.jmx.NetworkConnectorViewMBean;
import org.apache.activemq.broker.jmx.ProducerViewMBean;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.broker.jmx.SubscriptionViewMBean;
import org.apache.activemq.broker.jmx.TopicViewMBean;
import org.apache.activemq.web.util.ExceptionUtils;
import org.springframework.util.StringUtils;

/**
 * A useful base class for an implementation of {@link BrokerFacade}
 *
 *
 */
public abstract class BrokerFacadeSupport implements BrokerFacade {
    public abstract ManagementContext getManagementContext();
    public abstract Set queryNames(ObjectName name, QueryExp query) throws Exception;
    public abstract Object newProxyInstance( ObjectName objectName, Class interfaceClass, boolean notificationBroadcaster) throws Exception;

    @Override
    public Collection<QueueViewMBean> getQueues() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] queues = broker.getQueues();
        return getManagedObjects(queues, QueueViewMBean.class);
    }

    @Override
    public Collection<TopicViewMBean> getTopics() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] topics = broker.getTopics();
        return getManagedObjects(topics, TopicViewMBean.class);
    }

    @Override
    public Collection<SubscriptionViewMBean> getTopicSubscribers(String topicName) throws Exception {
        String brokerName = getBrokerName();
        topicName = StringUtils.replace(topicName, "\"", "_");
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName
                + ",destinationType=Topic,destinationName=" + topicName + ",endpoint=Consumer,*");
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]), SubscriptionViewMBean.class);
    }

    @Override
    public Collection<SubscriptionViewMBean> getNonDurableTopicSubscribers() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] subscribers = broker.getTopicSubscribers();
        return getManagedObjects(subscribers, SubscriptionViewMBean.class);
    }

    @Override
    public Collection<DurableSubscriptionViewMBean> getDurableTopicSubscribers() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] subscribers = broker.getDurableTopicSubscribers();
        return getManagedObjects(subscribers, DurableSubscriptionViewMBean.class);
    }

    @Override
    public Collection<DurableSubscriptionViewMBean> getInactiveDurableTopicSubscribers() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] subscribers = broker.getInactiveDurableTopicSubscribers();
        return getManagedObjects(subscribers, DurableSubscriptionViewMBean.class);
    }

    @Override
    public QueueViewMBean getQueue(String name) throws Exception {
        return (QueueViewMBean) getDestinationByName(getQueues(), name);
    }

    @Override
    public TopicViewMBean getTopic(String name) throws Exception {
        return (TopicViewMBean) getDestinationByName(getTopics(), name);
    }

    protected DestinationViewMBean getDestinationByName(Collection<? extends DestinationViewMBean> collection,
            String name) {
        Iterator<? extends DestinationViewMBean> iter = collection.iterator();
        while (iter.hasNext()) {
            try {
                DestinationViewMBean destinationViewMBean = iter.next();
                if (name.equals(destinationViewMBean.getName())) {
                    return destinationViewMBean;
                }
            } catch (Exception ex) {
                if (!ExceptionUtils.isRootCause(ex, InstanceNotFoundException.class)) {
                    // Only throw if not an expected InstanceNotFoundException exception
                    throw ex;
                }
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    protected <T> Collection<T> getManagedObjects(ObjectName[] names, Class<T> type) throws Exception {
        List<T> answer = new ArrayList<T>();
        for (int i = 0; i < names.length; i++) {
            ObjectName name = names[i];
            T value = (T) newProxyInstance(name, type, true);
            if (value != null) {
                answer.add(value);
            }
        }
        return answer;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<ConnectionViewMBean> getConnections() throws Exception {
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName + ",connector=clientConnectors,connectorName=*,connectionName=*");

        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]), ConnectionViewMBean.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<String> getConnections(String connectorName) throws Exception {
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName
            + ",connector=clientConnectors,connectorName=" + connectorName + ",connectionViewType=clientId" + ",connectionName=*");        Set<ObjectName> queryResult = queryNames(query, null);
        Collection<String> result = new ArrayList<String>(queryResult.size());
        for (ObjectName on : queryResult) {
            String name = StringUtils.replace(on.getKeyProperty("connectionName"), "_", ":");
            result.add(name);
        }
        return result;
    }

    @Override
    @SuppressWarnings("unchecked")
    public ConnectionViewMBean getConnection(String connectionName) throws Exception {
        connectionName = StringUtils.replace(connectionName, ":", "_");
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName
                + ",connector=clientConnectors,*,connectionName=" + connectionName);
        Set<ObjectName> queryResult = queryNames(query, null);
        if (queryResult.size() == 0)
            return null;
        ObjectName objectName = queryResult.iterator().next();
        return (ConnectionViewMBean) newProxyInstance(objectName, ConnectionViewMBean.class,
                true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<String> getConnectors() throws Exception {
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName + ",connector=clientConnectors,connectorName=*");
        Set<ObjectName> queryResult = queryNames(query, null);
        Collection<String> result = new ArrayList<String>(queryResult.size());
        for (ObjectName on : queryResult)
            result.add(on.getKeyProperty("connectorName"));
        return result;
    }

    @Override
    public ConnectorViewMBean getConnector(String name) throws Exception {
        String brokerName = getBrokerName();
        ObjectName objectName = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName
                + ",connector=clientConnectors,connectorName=" + name);
        return (ConnectorViewMBean) newProxyInstance(objectName, ConnectorViewMBean.class, true);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<NetworkConnectorViewMBean> getNetworkConnectors() throws Exception {
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName + ",connector=networkConnectors,networkConnectorName=*");
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]),
                NetworkConnectorViewMBean.class);
    }

    @Override
    public Collection<NetworkBridgeViewMBean> getNetworkBridges() throws Exception {
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName + ",connector=networkConnectors,networkConnectorName=*,networkBridge=*");
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]),
                NetworkBridgeViewMBean.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<SubscriptionViewMBean> getQueueConsumers(String queueName) throws Exception {
        String brokerName = getBrokerName();
        queueName = StringUtils.replace(queueName, "\"", "_");
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName
                + ",destinationType=Queue,destinationName=" + queueName + ",endpoint=Consumer,*");
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]), SubscriptionViewMBean.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<ProducerViewMBean> getQueueProducers(String queueName) throws Exception {
        String brokerName = getBrokerName();
        queueName = StringUtils.replace(queueName, "\"", "_");
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName
                + ",destinationType=Queue,destinationName=" + queueName + ",endpoint=Producer,*");
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]), ProducerViewMBean.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<ProducerViewMBean> getTopicProducers(String topicName) throws Exception {
        String brokerName = getBrokerName();
        topicName = StringUtils.replace(topicName, "\"", "_");
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName
                + ",destinationType=Topic,destinationName=" + topicName + ",endpoint=Producer,*");
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]), ProducerViewMBean.class);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Collection<SubscriptionViewMBean> getConsumersOnConnection(String connectionName) throws Exception {
        connectionName = StringUtils.replace(connectionName, ":", "_");
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:type=Broker,brokerName=" + brokerName
                + ",*,endpoint=Consumer,clientId=" + connectionName);
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]), SubscriptionViewMBean.class);
    }

    @Override
    public JobSchedulerViewMBean getJobScheduler() throws Exception {
        ObjectName name = getBrokerAdmin().getJMSJobScheduler();
        return (JobSchedulerViewMBean) newProxyInstance(name, JobSchedulerViewMBean.class, true);
    }

    @Override
    public Collection<JobFacade> getScheduledJobs() throws Exception {
        JobSchedulerViewMBean jobScheduler = getJobScheduler();
        List<JobFacade> result = new ArrayList<JobFacade>();
        TabularData table = jobScheduler.getAllJobs();
        for (Object object : table.values()) {
            CompositeData cd = (CompositeData) object;
            JobFacade jf = new JobFacade(cd);
            result.add(jf);
        }
        return result;
    }


    @Override
    public boolean isJobSchedulerStarted() {
        try {
            JobSchedulerViewMBean jobScheduler = getJobScheduler();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
