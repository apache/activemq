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

import java.util.Collection;

import org.apache.activemq.broker.jmx.*;
import org.apache.activemq.command.ActiveMQDestination;

/**
 * A facade for either a local in JVM broker or a remote broker over JMX
 *
 * 
 * @version $Revision$
 */
public interface BrokerFacade {

	/**
	 * The name of the active broker (f.e. 'localhost' or 'my broker').
	 * 
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	String getBrokerName() throws Exception;

	/**
	 * Admin view of the broker.
	 * 
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	BrokerViewMBean getBrokerAdmin() throws Exception;

	/**
	 * All queues known to the broker.
	 * 
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	Collection<QueueViewMBean> getQueues() throws Exception;

	/**
	 * All topics known to the broker.
	 * 
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	Collection<TopicViewMBean> getTopics() throws Exception;

	/**
	 * All active consumers of a queue.
	 * 
	 * @param queueName
	 *            the name of the queue, not <code>null</code>
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	Collection<SubscriptionViewMBean> getQueueConsumers(String queueName)
			throws Exception;

	/**
	 * Active durable subscribers to topics of the broker.
	 * 
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	Collection<DurableSubscriptionViewMBean> getDurableTopicSubscribers()
			throws Exception;


	/**
	 * Inactive durable subscribers to topics of the broker.
	 *
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	Collection<DurableSubscriptionViewMBean> getInactiveDurableTopicSubscribers()
			throws Exception;

	/**
	 * The names of all transport connectors of the broker (f.e. openwire, ssl)
	 * 
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	Collection<String> getConnectors() throws Exception;

	/**
	 * A transport connectors.
	 * 
	 * @param name
	 *            name of the connector (f.e. openwire)
	 * @return <code>null</code> if not found
	 * @throws Exception
	 */
	ConnectorViewMBean getConnector(String name) throws Exception;

	/**
	 * All connections to all transport connectors of the broker.
	 * 
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	Collection<ConnectionViewMBean> getConnections() throws Exception;

	/**
	 * The names of all connections to a specific transport connectors of the
	 * broker.
	 * 
	 * @see #getConnection(String)
	 * @param connectorName
	 *            not <code>null</code>
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	Collection<String> getConnections(String connectorName) throws Exception;

	/**
	 * A specific connection to the broker.
	 * 
	 * @param connectionName
	 *            the name of the connection, not <code>null</code>
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	ConnectionViewMBean getConnection(String connectionName) throws Exception;
	/**
	 * Returns all consumers of a connection.
	 * 
	 * @param connectionName
	 *            the name of the connection, not <code>null</code>
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	Collection<SubscriptionViewMBean> getConsumersOnConnection(
			String connectionName) throws Exception;
	/**
	 * The brokers network connectors.
	 * 
	 * @return not <code>null</code>
	 * @throws Exception
	 */
	Collection<NetworkConnectorViewMBean> getNetworkConnectors()
			throws Exception;


	/**
	 * The brokers network bridges.
	 *
	 * @return not <code>null</code>
	 * @throws Exception
	 */
    Collection<NetworkBridgeViewMBean> getNetworkBridges()
            throws Exception;

    /**
	 * Purges the given destination
	 * 
	 * @param destination
	 * @throws Exception
	 */
	void purgeQueue(ActiveMQDestination destination) throws Exception;
	/**
	 * Get the view of the queue with the specified name.
	 * 
	 * @param name
	 *            not <code>null</code>
	 * @return <code>null</code> if no queue with this name exists
	 * @throws Exception
	 */
	QueueViewMBean getQueue(String name) throws Exception;
	/**
	 * Get the view of the topic with the specified name.
	 * 
	 * @param name
	 *            not <code>null</code>
	 * @return <code>null</code> if no topic with this name exists
	 * @throws Exception
	 */
	TopicViewMBean getTopic(String name) throws Exception;
	
	/**
	 * Get the JobScheduler MBean
	 * @return the jobScheduler or null if not configured
	 * @throws Exception
	 */
	JobSchedulerViewMBean getJobScheduler() throws Exception;
	
	/**
     * Get the JobScheduler MBean
     * @return the jobScheduler or null if not configured
     * @throws Exception
     */
    Collection<JobFacade> getScheduledJobs() throws Exception;

    boolean isJobSchedulerStarted();

}