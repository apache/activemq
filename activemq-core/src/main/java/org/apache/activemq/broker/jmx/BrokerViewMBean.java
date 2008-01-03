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

import javax.management.ObjectName;

import org.apache.activemq.Service;

public interface BrokerViewMBean extends Service {

    /**
     * @return The unique id of the broker.
     */
    String getBrokerId();
    
    /**
     * @return The name of the broker.
     */
    String getBrokerName();    

    /**
     * The Broker will fush it's caches so that the garbage collector can
     * recalaim more memory.
     * 
     * @throws Exception
     */
    void gc() throws Exception;

    void resetStatistics();

    void enableStatistics();

    void disableStatistics();

    boolean isStatisticsEnabled();

    long getTotalEnqueueCount();

    long getTotalDequeueCount();

    long getTotalConsumerCount();

    long getTotalMessageCount();

    int getMemoryPercentageUsed();

    long getMemoryLimit();

    void setMemoryLimit(long limit);
    
    boolean isPersistent();

    /**
     * Shuts down the JVM.
     * 
     * @param exitCode the exit code that will be reported by the JVM process
     *                when it exits.
     */
    void terminateJVM(int exitCode);

    /**
     * Stop the broker and all it's components.
     */
    void stop() throws Exception;

    ObjectName[] getTopics();

    ObjectName[] getQueues();

    ObjectName[] getTemporaryTopics();

    ObjectName[] getTemporaryQueues();

    ObjectName[] getTopicSubscribers();

    ObjectName[] getDurableTopicSubscribers();

    ObjectName[] getInactiveDurableTopicSubscribers();

    ObjectName[] getQueueSubscribers();

    ObjectName[] getTemporaryTopicSubscribers();

    ObjectName[] getTemporaryQueueSubscribers();

    String addConnector(String discoveryAddress) throws Exception;

    String addNetworkConnector(String discoveryAddress) throws Exception;

    boolean removeConnector(String connectorName) throws Exception;

    boolean removeNetworkConnector(String connectorName) throws Exception;

    /**
     * Adds a Topic destination to the broker.
     * 
     * @param name The name of the Topic
     * @throws Exception
     */
    void addTopic(String name) throws Exception;

    /**
     * Adds a Queue destination to the broker.
     * 
     * @param name The name of the Queue
     * @throws Exception
     */
    void addQueue(String name) throws Exception;

    /**
     * Removes a Topic destination from the broker.
     * 
     * @param name The name of the Topic
     * @throws Exception
     */
    void removeTopic(String name) throws Exception;

    /**
     * Removes a Queue destination from the broker.
     * 
     * @param name The name of the Queue
     * @throws Exception
     */
    void removeQueue(String name) throws Exception;

    /**
     * Creates a new durable topic subscriber
     * 
     * @param clientId the JMS client ID
     * @param subscriberName the durable subscriber name
     * @param topicName the name of the topic to subscribe to
     * @param selector a selector or null
     * @return the object name of the MBean registered in JMX
     */
    ObjectName createDurableSubscriber(String clientId, String subscriberName, String topicName, String selector) throws Exception;

    /**
     * Destroys a durable subscriber
     * 
     * @param clientId the JMS client ID
     * @param subscriberName the durable subscriber name
     */
    void destroyDurableSubscriber(String clientId, String subscriberName) throws Exception;

    /**
     * Reloads log4j.properties from the classpath.
     * This methods calls org.apache.activemq.transport.TransportLoggerControl.reloadLog4jProperties
     * @throws Exception
     */
    public void reloadLog4jProperties() throws Exception;
    
}
