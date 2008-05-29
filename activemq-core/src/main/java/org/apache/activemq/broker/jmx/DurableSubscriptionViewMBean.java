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
 * @version $Revision: 1.5 $
 */
public interface DurableSubscriptionViewMBean extends SubscriptionViewMBean {
    /**
     * @return name of the durable subscription name
     */
    String getSubscriptionName();

    /**
     * Browse messages for this durable subscriber
     * 
     * @return messages
     * @throws OpenDataException
     */
    CompositeData[] browse() throws OpenDataException;

    /**
     * Browse messages for this durable subscriber
     * 
     * @return messages
     * @throws OpenDataException
     */
    TabularData browseAsTable() throws OpenDataException;

    /**
     * Destroys the durable subscription so that messages will no longer be
     * stored for this subscription
     */
    void destroy() throws Exception;
    
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
