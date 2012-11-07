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

import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;

public interface QueueViewMBean extends DestinationViewMBean {

    /**
     * Retrieve a message from the destination's queue.
     * 
     * @param messageId the message id of the message to retrieve
     * @return A CompositeData object which is a JMX version of the messages
     * @throws OpenDataException
     */
    @MBeanInfo("View a message from the destination by JMS message ID.")
    CompositeData getMessage(@MBeanInfo("messageId") String messageId) throws OpenDataException;

    /**
     * Removes a message from the queue. If the message has already been
     * dispatched to another consumer, the message cannot be deleted and this
     * method will return false.
     * 
     * @param messageId
     * @return true if the message was found and could be successfully deleted.
     * @throws Exception
     */
    @MBeanInfo("Remove a message from the destination by JMS message ID.  If the message has been dispatched, it cannot be deleted and false is returned.")
    boolean removeMessage(@MBeanInfo("messageId") String messageId) throws Exception;

    /**
     * Removes the messages matching the given selector
     * 
     * @return the number of messages removed
     */
    @MBeanInfo("Removes messages from the destination based on an SQL-92 selection on the message headers or XPATH on the body.")
    int removeMatchingMessages(@MBeanInfo("selector") String selector) throws Exception;

    /**
     * Removes the messages matching the given selector up to the maximum number
     * of matched messages
     * 
     * @return the number of messages removed
     */
    @MBeanInfo("Removes up to a specified number of messages from the destination based on an SQL-92 selection on the message headers or XPATH on the body.")
    int removeMatchingMessages(@MBeanInfo("selector") String selector, @MBeanInfo("maximumMessages") int maximumMessages) throws Exception;

    /**
     * Removes all of the messages in the queue.
     * 
     * @throws Exception
     */
    @MBeanInfo("Removes all of the messages in the queue.")
    void purge() throws Exception;

    /**
     * Copies a given message to another destination.
     * 
     * @param messageId
     * @param destinationName
     * @return true if the message was found and was successfully copied to the
     *         other destination.
     * @throws Exception
     */
    @MBeanInfo("Copies a message with the given JMS message ID into the specified destination.")
    boolean copyMessageTo(@MBeanInfo("messageId") String messageId, @MBeanInfo("destinationName") String destinationName) throws Exception;

    /**
     * Copies the messages matching the given selector
     * 
     * @return the number of messages copied
     */
    @MBeanInfo("Copies messages based on an SQL-92 selecton on the message headers or XPATH on the body into the specified destination.")
    int copyMatchingMessagesTo(@MBeanInfo("selector") String selector, @MBeanInfo("destinationName") String destinationName) throws Exception;

    /**
     * Copies the messages matching the given selector up to the maximum number
     * of matched messages
     * 
     * @return the number of messages copied
     */
    @MBeanInfo("Copies up to a specified number of messages based on an SQL-92 selecton on the message headers or XPATH on the body into the specified destination.")
    int copyMatchingMessagesTo(@MBeanInfo("selector") String selector, @MBeanInfo("destinationName") String destinationName, @MBeanInfo("maximumMessages") int maximumMessages) throws Exception;

    /**
     * Moves the message to another destination.
     * 
     * @param messageId
     * @param destinationName
     * @return true if the message was found and was successfully copied to the
     *         other destination.
     * @throws Exception
     */
    @MBeanInfo("Moves a message with the given JMS message ID into the specified destination.")
    boolean moveMessageTo(@MBeanInfo("messageId") String messageId, @MBeanInfo("destinationName") String destinationName) throws Exception;

    /**
     * Moves a message back to its original destination
     */
    @MBeanInfo("Moves a message with the given JMS message back to its original destination")
    boolean retryMessage(@MBeanInfo("messageId") String messageId) throws Exception;
    
    /**
     * Moves the messages matching the given selector
     * 
     * @return the number of messages removed
     */
    @MBeanInfo("Moves messages based on an SQL-92 selecton on the message headers or XPATH on the body into the specified destination.")
    int moveMatchingMessagesTo(@MBeanInfo("selector") String selector, @MBeanInfo("destinationName") String destinationName) throws Exception;

    /**
     * Moves the messages matching the given selector up to the maximum number
     * of matched messages
     */
    @MBeanInfo("Moves up to a specified number of messages based on an SQL-92 selecton on the message headers or XPATH on the body into the specified destination.")
    int moveMatchingMessagesTo(@MBeanInfo("selector") String selector, @MBeanInfo("destinationName") String destinationName, @MBeanInfo("maximumMessages") int maximumMessages) throws Exception;
    
    /**
     * @return true if the message cursor has memory space available
     * to page in more messages
     */
    @MBeanInfo("Message cursor has memory space available")
    public boolean doesCursorHaveSpace();
    
    /**
     * @return true if the cursor has reached its memory limit for
     * paged in messages
     */
    @MBeanInfo("Message cusor has reached its memory limit for paged in messages")
    public boolean isCursorFull();
    
    /**
     * @return true if the cursor has messages buffered to deliver
     */
    @MBeanInfo("Message cursor has buffered messages to deliver")
    public boolean doesCursorHaveMessagesBuffered();
    
    /**
     * @return the cursor memory usage in bytes
     */
    @MBeanInfo("Message cursor memory usage, in bytes.")
    public long getCursorMemoryUsage();
    
    /**
     * @return the cursor memory usage as a percentage
     */
    @MBeanInfo("Percentage of memory limit used")
    public int getCursorPercentUsage();
    
    /**
     * @return the number of messages available to be paged in 
     * by the cursor
     */
    @MBeanInfo("Number of messages available to be paged in by the cursor.")
    public int cursorSize();

    /**
     * @return true if caching is currently enabled of for the destination
     */
    @MBeanInfo("Caching is enabled")
    boolean isCacheEnabled();
}
