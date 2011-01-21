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
import java.util.List;
import java.util.Map;
import javax.jms.InvalidSelectorException;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;

public interface DestinationViewMBean {

    /**
     * Returns the name of this destination
     */
    @MBeanInfo("Name of this destination.")
    String getName();

    /**
     * Resets the managment counters.
     */
    @MBeanInfo("Resets statistics.")
    void resetStatistics();

    /**
     * Returns the number of messages that have been sent to the destination.
     * 
     * @return The number of messages that have been sent to the destination.
     */
    @MBeanInfo("Number of messages that have been sent to the destination.")
    long getEnqueueCount();

    /**
     * Returns the number of messages that have been delivered (potentially not
     * acknowledged) to consumers.
     * 
     * @return The number of messages that have been delivered (potentially not
     *         acknowledged) to consumers.
     */
    @MBeanInfo("Number of messages that have been delivered (but potentially not acknowledged) to consumers.")
    long getDispatchCount();

    /**
     * Returns the number of messages that have been acknowledged from the
     * destination.
     * 
     * @return The number of messages that have been acknowledged from the
     *         destination.
     */
    @MBeanInfo("Number of messages that have been acknowledged (and removed from) from the destination.")
    long getDequeueCount();
    
    /**
     * Returns the number of messages that have been dispatched but not
     * acknowledged
     * 
     * @return The number of messages that have been dispatched but not
     * acknowledged
     */
    @MBeanInfo("Number of messages that have been dispatched to, but not acknowledged by, consumers.")
    long getInFlightCount();

    /**
     * Returns the number of messages that have expired
     * 
     * @return The number of messages that have expired
     */
    @MBeanInfo("Number of messages that have been expired.")
    long getExpiredCount();
    
    /**
     * Returns the number of consumers subscribed this destination.
     * 
     * @return The number of consumers subscribed this destination.
     */
    @MBeanInfo("Number of consumers subscribed to this destination.")
    long getConsumerCount();
    
    /**
     * @return the number of producers publishing to the destination
     */
    @MBeanInfo("Number of producers publishing to this destination")
    long getProducerCount();

    /**
     * Returns the number of messages in this destination which are yet to be
     * consumed
     * 
     * @return Returns the number of messages in this destination which are yet
     *         to be consumed
     */
    @MBeanInfo("Number of messages in the destination which are yet to be consumed.  Potentially dispatched but unacknowledged.")
    long getQueueSize();

    /**
     * @return An array of all the messages in the destination's queue.
     */
    @MBeanInfo("An array of all messages in the destination. Not HTML friendly.")
    CompositeData[] browse() throws OpenDataException;

    /**
     * @return A list of all the messages in the destination's queue.
     */
    @MBeanInfo("A list of all messages in the destination. Not HTML friendly.")
    TabularData browseAsTable() throws OpenDataException;

    /**
     * @return An array of all the messages in the destination's queue.
     * @throws InvalidSelectorException
     */
    @MBeanInfo("An array of all messages in the destination based on an SQL-92 selection on the message headers or XPATH on the body. Not HTML friendly.")
    CompositeData[] browse(@MBeanInfo("selector") String selector) throws OpenDataException, InvalidSelectorException;

    /**
     * @return A list of all the messages in the destination's queue.
     * @throws InvalidSelectorException
     */
    @MBeanInfo("A list of all messages in the destination based on an SQL-92 selection on the message headers or XPATH on the body. Not HTML friendly.")
    TabularData browseAsTable(@MBeanInfo("selector") String selector) throws OpenDataException, InvalidSelectorException;

    /**
     * Sends a TextMesage to the destination.
     * 
     * @param body the text to send
     * @return the message id of the message sent.
     * @throws Exception
     */
    @MBeanInfo("Sends a TextMessage to the destination.")
    String sendTextMessage(@MBeanInfo("body") String body) throws Exception;

    /**
     * Sends a TextMesage to the destination.
     * 
     * @param headers the message headers and properties to set. Can only
     *                container Strings maped to primitive types.
     * @param body the text to send
     * @return the message id of the message sent.
     * @throws Exception
     */
    @MBeanInfo("Sends a TextMessage to the destination.")
    String sendTextMessage(@MBeanInfo("headers") Map<?,?> headers, @MBeanInfo("body") String body) throws Exception;

    /**
     * Sends a TextMesage to the destination.
     * @param body the text to send
     * @param user
     * @param password
     * @return
     * @throws Exception
     */
    @MBeanInfo("Sends a TextMessage to a password-protected destination.")
    String sendTextMessage(@MBeanInfo("body") String body, @MBeanInfo("user") String user, @MBeanInfo("password") String password) throws Exception;
    
    /**
     * 
     * @param headers the message headers and properties to set. Can only
     *                container Strings maped to primitive types.
     * @param body the text to send
     * @param user
     * @param password
     * @return
     * @throws Exception
     */
    @MBeanInfo("Sends a TextMessage to a password-protected destination.")
    String sendTextMessage(@MBeanInfo("headers") Map<?,?> headers, @MBeanInfo("body") String body, @MBeanInfo("user") String user, @MBeanInfo("password") String password) throws Exception;
    /**
     * @return the percentage of amount of memory used
     */
    @MBeanInfo("The percentage of the memory limit used")
    int getMemoryPercentUsage();

    /**
     * @return the amount of memory allocated to this destination
     */
    @MBeanInfo("Memory limit, in bytes, used for holding undelivered messages before paging to temporary storage.")
    long getMemoryLimit();

    /**
     * set the amount of memory allocated to this destination
     * @param limit
     */
    void setMemoryLimit(long limit);
    
    /**
     * @return the portion of memory from the broker memory limit for this destination
     */
    @MBeanInfo("Portion of memory from the broker memory limit for this destination")
    float getMemoryUsagePortion();
    
    /**
     * set the portion of memory from the broker memory limit for this destination
     * @param value
     */
    void setMemoryUsagePortion(@MBeanInfo("bytes") float value);

    /**
     * Browses the current destination returning a list of messages
     */
    @MBeanInfo("A list of all messages in the destination. Not HTML friendly.")
    List<?> browseMessages() throws InvalidSelectorException;

    /**
     * Browses the current destination with the given selector returning a list
     * of messages
     */
    @MBeanInfo("A list of all messages in the destination based on an SQL-92 selection on the message headers or XPATH on the body. Not HTML friendly.")
    List<?> browseMessages(String selector) throws InvalidSelectorException;

    /**
     * @return longest time a message is held by a destination
     */
    @MBeanInfo("The longest time a message has been held this destination.")
    long getMaxEnqueueTime();

    /**
     * @return shortest time a message is held by a destination
     */
    @MBeanInfo("The shortest time a message has been held this destination.")
    long getMinEnqueueTime();

    /**
     * @return average time a message is held by a destination
     */
    @MBeanInfo("Average time a message has been held this destination.")
    double getAverageEnqueueTime();
    
    /**
     * @return the producerFlowControl
     */
    @MBeanInfo("Producers are flow controlled")
    boolean isProducerFlowControl();
    
    /**
     * @param producerFlowControl the producerFlowControl to set
     */
    public void setProducerFlowControl(@MBeanInfo("producerFlowControl") boolean producerFlowControl);
    
    /**
     * Set's the interval at which warnings about producers being blocked by
     * resource usage will be triggered. Values of 0 or less will disable
     * warnings
     * 
     * @param blockedProducerWarningInterval the interval at which warning about
     *            blocked producers will be triggered.
     */
    public void setBlockedProducerWarningInterval(@MBeanInfo("blockedProducerWarningInterval")  long blockedProducerWarningInterval);

    /**
     * 
     * @return the interval at which warning about blocked producers will be
     *         triggered.
     */
    @MBeanInfo("Blocked Producer Warning Interval")
    public long getBlockedProducerWarningInterval();
    
    /**
     * @return the maxProducersToAudit
     */
    @MBeanInfo("Maximum number of producers to audit") 
    public int getMaxProducersToAudit();
    
    /**
     * @param maxProducersToAudit the maxProducersToAudit to set
     */
    public void setMaxProducersToAudit(@MBeanInfo("maxProducersToAudit") int maxProducersToAudit);
    
    /**
     * @return the maxAuditDepth
     */
    @MBeanInfo("Max audit depth")
    public int getMaxAuditDepth();
    
    /**
     * @param maxAuditDepth the maxAuditDepth to set
     */
    public void setMaxAuditDepth(@MBeanInfo("maxAuditDepth") int maxAuditDepth);
    
    /**
     * @return the maximum number of message to be paged into the 
     * destination
     */
    @MBeanInfo("Maximum number of messages to be paged in")
    public int getMaxPageSize();
    
    /**
     * @param pageSize
     * Set the maximum number of messages to page into the destination
     */
    public void setMaxPageSize(@MBeanInfo("pageSize") int pageSize);
    
    /**
     * @return true if caching is allowed of for the destination
     */
    @MBeanInfo("Caching is allowed")
    public boolean isUseCache();
    
    /**
     * @return true if prioritized messages are enabled for the destination
     */
    @MBeanInfo("Prioritized messages is enabled")
    public boolean isPrioritizedMessages();
    
    /**
     * @param value
     * enable/disable caching on the destination
     */
    public void setUseCache(@MBeanInfo("cache") boolean value);

    /**
     * Returns all the current subscription MBeans matching this destination
     * 
     * @return the names of the subscriptions for this destination
     */
    @MBeanInfo("returns all the current subscription MBeans matching this destination")
    ObjectName[] getSubscriptions() throws IOException, MalformedObjectNameException;


    /**
     * Returns the slow consumer strategy MBean for this destination
     *
     * @return the name of the slow consumer handler MBean for this destination
     */
    @MBeanInfo("returns the optional slowConsumer handler MBeans for this destination")
    ObjectName getSlowConsumerStrategy() throws IOException, MalformedObjectNameException;

}
