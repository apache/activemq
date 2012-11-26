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
import javax.management.*;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.TabularData;

import org.apache.activemq.broker.jmx.*;
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

    public Collection<QueueViewMBean> getQueues() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] queues = broker.getQueues();
        return getManagedObjects(queues, QueueViewMBean.class);
    }

    public Collection<TopicViewMBean> getTopics() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] queues = broker.getTopics();
        return getManagedObjects(queues, TopicViewMBean.class);
    }

    public Collection<DurableSubscriptionViewMBean> getDurableTopicSubscribers() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] queues = broker.getDurableTopicSubscribers();
        return getManagedObjects(queues, DurableSubscriptionViewMBean.class);
    }

    public Collection<DurableSubscriptionViewMBean> getInactiveDurableTopicSubscribers() throws Exception {
        BrokerViewMBean broker = getBrokerAdmin();
        if (broker == null) {
            return Collections.EMPTY_LIST;
        }
        ObjectName[] queues = broker.getInactiveDurableTopicSubscribers();
        return getManagedObjects(queues, DurableSubscriptionViewMBean.class);
    }

    public QueueViewMBean getQueue(String name) throws Exception {
        return (QueueViewMBean) getDestinationByName(getQueues(), name);
    }

    public TopicViewMBean getTopic(String name) throws Exception {
        return (TopicViewMBean) getDestinationByName(getTopics(), name);
    }

    protected DestinationViewMBean getDestinationByName(Collection<? extends DestinationViewMBean> collection,
            String name) {
        Iterator<? extends DestinationViewMBean> iter = collection.iterator();
        while (iter.hasNext()) {
            DestinationViewMBean destinationViewMBean = iter.next();
            if (name.equals(destinationViewMBean.getName())) {
                return destinationViewMBean;
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

    @SuppressWarnings("unchecked")
    public Collection<ConnectionViewMBean> getConnections() throws Exception {
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:BrokerName=" + brokerName + ",Type=Connection,ConnectorName=*,Connection=*");

        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]), ConnectionViewMBean.class);
    }

    @SuppressWarnings("unchecked")
    public Collection<String> getConnections(String connectorName) throws Exception {
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:BrokerName=" + brokerName
                + ",Type=Connection,ConnectorName=" + connectorName + ",Connection=*");
        Set<ObjectName> queryResult = queryNames(query, null);
        Collection<String> result = new ArrayList<String>(queryResult.size());
        for (ObjectName on : queryResult) {
            String name = StringUtils.replace(on.getKeyProperty("Connection"), "_", ":");
            result.add(name);
        }
        return result;
    }

    @SuppressWarnings("unchecked")
    public ConnectionViewMBean getConnection(String connectionName) throws Exception {
        connectionName = StringUtils.replace(connectionName, ":", "_");
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:BrokerName=" + brokerName
                + ",Type=Connection,*,Connection=" + connectionName);
        Set<ObjectName> queryResult = queryNames(query, null);
        if (queryResult.size() == 0)
            return null;
        ObjectName objectName = queryResult.iterator().next();
        return (ConnectionViewMBean) newProxyInstance(objectName, ConnectionViewMBean.class,
                true);
    }

    @SuppressWarnings("unchecked")
    public Collection<String> getConnectors() throws Exception {
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:BrokerName=" + brokerName + ",Type=Connector,*");
        Set<ObjectName> queryResult = queryNames(query, null);
        Collection<String> result = new ArrayList<String>(queryResult.size());
        for (ObjectName on : queryResult)
            result.add(on.getKeyProperty("ConnectorName"));
        return result;
    }

    public ConnectorViewMBean getConnector(String name) throws Exception {
        String brokerName = getBrokerName();
        ObjectName objectName = new ObjectName("org.apache.activemq:BrokerName=" + brokerName
                + ",Type=Connector,ConnectorName=" + name);
        return (ConnectorViewMBean) newProxyInstance(objectName, ConnectorViewMBean.class, true);
    }

    @SuppressWarnings("unchecked")
    public Collection<NetworkConnectorViewMBean> getNetworkConnectors() throws Exception {
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:BrokerName=" + brokerName + ",Type=NetworkConnector,*");
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]),
                NetworkConnectorViewMBean.class);
    }

    public Collection<NetworkBridgeViewMBean> getNetworkBridges() throws Exception {
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:BrokerName=" + brokerName + ",Type=NetworkBridge,*");
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]),
                NetworkBridgeViewMBean.class);
    }

    @SuppressWarnings("unchecked")
    public Collection<SubscriptionViewMBean> getQueueConsumers(String queueName) throws Exception {
        String brokerName = getBrokerName();
        queueName = StringUtils.replace(queueName, "\"", "_");
        ObjectName query = new ObjectName("org.apache.activemq:BrokerName=" + brokerName
                + ",Type=Subscription,destinationType=Queue,destinationName=" + queueName + ",*");
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]), SubscriptionViewMBean.class);
    }

    @SuppressWarnings("unchecked")
    public Collection<SubscriptionViewMBean> getConsumersOnConnection(String connectionName) throws Exception {
        connectionName = StringUtils.replace(connectionName, ":", "_");
        String brokerName = getBrokerName();
        ObjectName query = new ObjectName("org.apache.activemq:BrokerName=" + brokerName
                + ",Type=Subscription,clientId=" + connectionName + ",*");
        Set<ObjectName> queryResult = queryNames(query, null);
        return getManagedObjects(queryResult.toArray(new ObjectName[queryResult.size()]), SubscriptionViewMBean.class);
    }

    public JobSchedulerViewMBean getJobScheduler() throws Exception {
        ObjectName name = getBrokerAdmin().getJMSJobScheduler();
        return (JobSchedulerViewMBean) newProxyInstance(name, JobSchedulerViewMBean.class, true);
    }

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


    public boolean isJobSchedulerStarted() {
        try {
            JobSchedulerViewMBean jobScheduler = getJobScheduler();
            return true;
        } catch (Exception e) {
            return false;
        }
    }
}
