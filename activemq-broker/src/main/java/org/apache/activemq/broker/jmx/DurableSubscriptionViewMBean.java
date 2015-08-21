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
import javax.management.openmbean.TabularData;

/**
 * 
 */
public interface DurableSubscriptionViewMBean extends SubscriptionViewMBean {
    /**
     * @return name of the durable subscription name
     */
    @MBeanInfo("The subscription name.")
    String getSubscriptionName();

    /**
     * Browse messages for this durable subscriber
     * 
     * @return messages
     * @throws OpenDataException
     */
    @MBeanInfo("Browse the composite data array of pending messages in this subscription")
    CompositeData[] browse() throws OpenDataException;

    /**
     * Browse messages for this durable subscriber
     * 
     * @return messages
     * @throws OpenDataException
     */
    @MBeanInfo("Browse the tabular data of pending messages in this subscription")
    TabularData browseAsTable() throws OpenDataException;

    /**
     * Destroys the durable subscription so that messages will no longer be
     * stored for this subscription
     */
    @MBeanInfo("Destroy or delete this subscription")
    void destroy() throws Exception;
    
    /**
     * @return true if the message cursor has memory space available
     * to page in more messages
     */
    @MBeanInfo("The subscription has space for more messages in memory")
    public boolean doesCursorHaveSpace();
    
    /**
     * @return true if the cursor has reached its memory limit for
     * paged in messages
     */
    @MBeanInfo("The subscription cursor is full")
    public boolean isCursorFull();
    
    /**
     * @return true if the cursor has messages buffered to deliver
     */
    @MBeanInfo("The subscription cursor has messages in memory")
    public boolean doesCursorHaveMessagesBuffered();
    
    /**
     * @return the cursor memory usage in bytes
     */
    @MBeanInfo("The subscription cursor memory usage bytes")
    public long getCursorMemoryUsage();
    
    /**
     * @return the cursor memory usage as a percentage
     */
    @MBeanInfo("The subscription cursor memory usage %")
    public int getCursorPercentUsage();
    
    /**
     * @return the number of messages available to be paged in 
     * by the cursor
     */
    @MBeanInfo("The subscription cursor size or message count")
    public int cursorSize();

    /**
     * Removes a message from the durable subscription.
     *
     * @param messageId
     * @throws Exception
     */
    @MBeanInfo("Remove a message from the subscription by JMS message ID.")
    public void removeMessage(@MBeanInfo("messageId") String messageId) throws Exception;
}
