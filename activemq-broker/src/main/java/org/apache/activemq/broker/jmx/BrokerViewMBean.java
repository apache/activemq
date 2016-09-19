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

import java.io.IOException;
import java.util.Map;

import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

import org.apache.activemq.Service;

public interface BrokerViewMBean extends Service {

    /**
     * @return The unique id of the broker.
     */
    @MBeanInfo("The unique id of the broker.")
    String getBrokerId();

    /**
     * @return The name of the broker.
     */
    @MBeanInfo("The name of the broker.")
    String getBrokerName();

    /**
     * @return The name of the broker.
     */
    @MBeanInfo("The version of the broker.")
    String getBrokerVersion();

    /**
     * @return Uptime of the broker.
     */
    @MBeanInfo("Uptime of the broker.")
    String getUptime();

    /**
     * @return Uptime of the broker in milliseconds.
     */
    @MBeanInfo("Uptime of the broker in milliseconds.")
    long getUptimeMillis();

    /**
     * @return The current number of active connections on this Broker.
     */
    int getCurrentConnectionsCount();

    /**
     * @return The total number of connections serviced since this Broker was started.
     */
    long getTotalConnectionsCount();

    /**
     * The Broker will flush it's caches so that the garbage collector can
     * reclaim more memory.
     *
     * @throws Exception
     */
    @MBeanInfo("Runs the Garbage Collector.")
    void gc() throws Exception;

    @MBeanInfo("Reset all broker statistics.")
    void resetStatistics();

    @MBeanInfo("Enable broker statistics.")
    void enableStatistics();

    @MBeanInfo("Disable broker statistics.")
    void disableStatistics();

    @MBeanInfo("Broker statistics enabled.")
    boolean isStatisticsEnabled();

    @MBeanInfo("Number of messages that have been sent to the broker.")
    long getTotalEnqueueCount();

    @MBeanInfo("Number of messages that have been acknowledged on the broker.")
    long getTotalDequeueCount();

    @MBeanInfo("Number of message consumers subscribed to destinations on the broker.")
    long getTotalConsumerCount();

    @MBeanInfo("Number of message producers active on destinations on the broker.")
    long getTotalProducerCount();

    @MBeanInfo("Number of unacknowledged messages on the broker.")
    long getTotalMessageCount();

    @MBeanInfo("Average message size on this broker")
    long getAverageMessageSize();

    @MBeanInfo("Max message size on this broker")
    public long getMaxMessageSize();

    @MBeanInfo("Min message size on this broker")
    public long getMinMessageSize();

    @MBeanInfo("Percent of memory limit used.")
    int getMemoryPercentUsage();

    @MBeanInfo("Memory limit, in bytes, used for holding undelivered messages before paging to temporary storage.")
    long getMemoryLimit();

    void setMemoryLimit(@MBeanInfo("bytes") long limit);

    @MBeanInfo("Percent of store limit used.")
    int getStorePercentUsage();

    @MBeanInfo("Disk limit, in bytes, used for persistent messages before producers are blocked.")
    long getStoreLimit();

    void setStoreLimit(@MBeanInfo("bytes") long limit);

    @MBeanInfo("Percent of temp limit used.")
    int getTempPercentUsage();

    @MBeanInfo("Disk limit, in bytes, used for non-persistent messages and temporary data before producers are blocked.")
    long getTempLimit();

    void setTempLimit(@MBeanInfo("bytes") long limit);

    @MBeanInfo("Percent of job store limit used.")
    int getJobSchedulerStorePercentUsage();

    @MBeanInfo("Disk limit, in bytes, used for scheduled messages before producers are blocked.")
    long getJobSchedulerStoreLimit();

    void setJobSchedulerStoreLimit(@MBeanInfo("bytes") long limit);

    @MBeanInfo("Messages are synchronized to disk.")
    boolean isPersistent();

    @MBeanInfo("Slave broker.")
    boolean isSlave();

    /**
     * Shuts down the JVM.
     *
     * @param exitCode the exit code that will be reported by the JVM process
     *                when it exits.
     */
    @MBeanInfo("Shuts down the JVM.")
    void terminateJVM(@MBeanInfo("exitCode") int exitCode);

    /**
     * Stop the broker and all it's components.
     */
    @Override
    @MBeanInfo("Stop the broker and all its components.")
    void stop() throws Exception;

    /**
     * Restart the broker and all it's components.
     */
    @MBeanInfo("Restart the broker and all its components.")
    void restart() throws Exception;

    @MBeanInfo("Poll for queues matching queueName are empty before stopping")
    void stopGracefully(String connectorName, String queueName, long timeout, long pollInterval) throws Exception;

    @MBeanInfo("Topics (broadcasted 'queues'); generally system information.")
    ObjectName[] getTopics();

    @MBeanInfo("Standard Queues containing AIE messages.")
    ObjectName[] getQueues();

    /**
     * Queue Query API, take a look at {@link DestinationsViewFilter} for more information
     */
    @MBeanInfo("Query queues")
    String queryQueues(String filter, int page, int pageSize) throws IOException;

    /**
     * Topic Query API, take a look at {@link DestinationsViewFilter} for more information
     */
    @MBeanInfo("Query topics")
    String queryTopics(String filter, int page, int pageSize) throws IOException;

    public CompositeData[] browseQueue(String queueName) throws OpenDataException, MalformedObjectNameException;

    @MBeanInfo("Temporary Topics; generally unused.")
    ObjectName[] getTemporaryTopics();

    @MBeanInfo("Temporary Queues; generally temporary message response holders.")
    ObjectName[] getTemporaryQueues();

    @MBeanInfo("Topic Subscribers")
    ObjectName[] getTopicSubscribers();

    @MBeanInfo("Durable (persistent) topic subscribers")
    ObjectName[] getDurableTopicSubscribers();

    @MBeanInfo("Inactive (disconnected persistent) topic subscribers")
    ObjectName[] getInactiveDurableTopicSubscribers();

    @MBeanInfo("Queue Subscribers.")
    ObjectName[] getQueueSubscribers();

    @MBeanInfo("Temporary Topic Subscribers.")
    ObjectName[] getTemporaryTopicSubscribers();

    @MBeanInfo("Temporary Queue Subscribers.")
    ObjectName[] getTemporaryQueueSubscribers();

    @MBeanInfo("Topic Producers.")
    public ObjectName[] getTopicProducers();

    @MBeanInfo("Queue Producers.")
    public ObjectName[] getQueueProducers();

    @MBeanInfo("Temporary Topic Producers.")
    public ObjectName[] getTemporaryTopicProducers();

    @MBeanInfo("Temporary Queue Producers.")
    public ObjectName[] getTemporaryQueueProducers();

    @MBeanInfo("Dynamic Destination Producers.")
    public ObjectName[] getDynamicDestinationProducers();

    @MBeanInfo("Adds a Connector to the broker.")
    String addConnector(@MBeanInfo("discoveryAddress") String discoveryAddress) throws Exception;

    @MBeanInfo("Adds a Network Connector to the broker.")
    String addNetworkConnector(@MBeanInfo("discoveryAddress") String discoveryAddress) throws Exception;

    @MBeanInfo("Removes a Connector from the broker.")
    boolean removeConnector(@MBeanInfo("connectorName") String connectorName) throws Exception;

    @MBeanInfo("Removes a Network Connector from the broker.")
    boolean removeNetworkConnector(@MBeanInfo("connectorName") String connectorName) throws Exception;

    /**
     * Adds a Topic destination to the broker.
     *
     * @param name The name of the Topic
     * @throws Exception
     */
    @MBeanInfo("Adds a Topic destination to the broker.")
    void addTopic(@MBeanInfo("name") String name) throws Exception;

    /**
     * Adds a Queue destination to the broker.
     *
     * @param name The name of the Queue
     * @throws Exception
     */
    @MBeanInfo("Adds a Queue destination to the broker.")
    void addQueue(@MBeanInfo("name") String name) throws Exception;

    /**
     * Removes a Topic destination from the broker.
     *
     * @param name The name of the Topic
     * @throws Exception
     */
    @MBeanInfo("Removes a Topic destination from the broker.")
    void removeTopic(@MBeanInfo("name") String name) throws Exception;

    /**
     * Removes a Queue destination from the broker.
     *
     * @param name The name of the Queue
     * @throws Exception
     */
    @MBeanInfo("Removes a Queue destination from the broker.")
    void removeQueue(@MBeanInfo("name") String name) throws Exception;

    /**
     * Creates a new durable topic subscriber
     *
     * @param clientId the JMS client ID
     * @param subscriberName the durable subscriber name
     * @param topicName the name of the topic to subscribe to
     * @param selector a selector or null
     * @return the object name of the MBean registered in JMX
     */
    @MBeanInfo(value="Creates a new durable topic subscriber.")
    ObjectName createDurableSubscriber(@MBeanInfo("clientId") String clientId, @MBeanInfo("subscriberName") String subscriberName, @MBeanInfo("topicName") String topicName, @MBeanInfo("selector") String selector) throws Exception;

    /**
     * Destroys a durable subscriber
     *
     * @param clientId the JMS client ID
     * @param subscriberName the durable subscriber name
     */
    @MBeanInfo(value="Destroys a durable subscriber.")
    void destroyDurableSubscriber(@MBeanInfo("clientId") String clientId, @MBeanInfo("subscriberName") String subscriberName) throws Exception;

    /**
     * Reloads log4j.properties from the classpath.
     * This methods calls org.apache.activemq.transport.TransportLoggerControl.reloadLog4jProperties
     * @throws Throwable
     */
    @MBeanInfo(value="Reloads log4j.properties from the classpath.")
    public void reloadLog4jProperties() throws Throwable;

    @MBeanInfo("The url of the VM connector")
    String getVMURL();

    @MBeanInfo("The map of all defined transport connectors, with transport name as a key")
    Map<String, String> getTransportConnectors();

    @MBeanInfo("The url of transport connector by it's type; e.g. tcp, stomp, ssl, etc.")
    String getTransportConnectorByType(String type);

    @MBeanInfo("The location of the data directory")
    public String getDataDirectory();

    @MBeanInfo("JMSJobScheduler")
    ObjectName getJMSJobScheduler();

}
