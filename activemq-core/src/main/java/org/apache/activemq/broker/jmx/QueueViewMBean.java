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
    CompositeData getMessage(String messageId) throws OpenDataException;

    /**
     * Removes a message from the queue. If the message has already been
     * dispatched to another consumer, the message cannot be deleted and this
     * method will return false.
     * 
     * @param messageId
     * @return true if the message was found and could be successfully deleted.
     * @throws Exception
     */
    boolean removeMessage(String messageId) throws Exception;

    /**
     * Removes the messages matching the given selector
     * 
     * @return the number of messages removed
     */
    int removeMatchingMessages(String selector) throws Exception;

    /**
     * Removes the messages matching the given selector up to the maximum number
     * of matched messages
     * 
     * @return the number of messages removed
     */
    int removeMatchingMessages(String selector, int maximumMessages) throws Exception;

    /**
     * Removes all of the messages in the queue.
     * 
     * @throws Exception
     */
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
    boolean copyMessageTo(String messageId, String destinationName) throws Exception;

    /**
     * Copies the messages matching the given selector
     * 
     * @return the number of messages copied
     */
    int copyMatchingMessagesTo(String selector, String destinationName) throws Exception;

    /**
     * Copies the messages matching the given selector up to the maximum number
     * of matched messages
     * 
     * @return the number of messages copied
     */
    int copyMatchingMessagesTo(String selector, String destinationName, int maximumMessages) throws Exception;

    /**
     * Moves the message to another destination.
     * 
     * @param messageId
     * @param destinationName
     * @return true if the message was found and was successfully copied to the
     *         other destination.
     * @throws Exception
     */
    boolean moveMessageTo(String messageId, String destinationName) throws Exception;

    /**
     * Moves a message back to its original destination
     */
    boolean retryMessage(String messageId) throws Exception;
    
    /**
     * Moves the messages matching the given selector
     * 
     * @return the number of messages removed
     */
    int moveMatchingMessagesTo(String selector, String destinationName) throws Exception;

    /**
     * Moves the messages matching the given selector up to the maximum number
     * of matched messages
     */
    int moveMatchingMessagesTo(String selector, String destinationName, int maximumMessages) throws Exception;
    
    /**
     * @return true if the message cursor has memory space available
     * to page in more messages
     */
    public boolean doesCursorHaveSpace();
    
    /**
     * @return true if the cursor has reached its memory limit for
     * paged in messages
     */
    public boolean isCursorFull();
    
    /**
     * @return true if the cursor has messages buffered to deliver
     */
    public boolean doesCursorHaveMessagesBuffered();
    
    /**
     * @return the cursor memory usage in bytes
     */
    public long getCursorMemoryUsage();
    
    /**
     * @return the cursor memory usage as a percentage
     */
    public int getCursorPercentUsage();
    
    /**
     * @return the number of messages available to be paged in 
     * by the cursor
     */
    public int cursorSize();

}
