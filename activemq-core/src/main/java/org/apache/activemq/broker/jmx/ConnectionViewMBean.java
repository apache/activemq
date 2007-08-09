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

import org.apache.activemq.Service;

public interface ConnectionViewMBean extends Service {
    /**
     * @return true if the Connection is slow
     */
    boolean isSlow();

    /**
     * @return if after being marked, the Connection is still writing
     */
    boolean isBlocked();

    /**
     * @return true if the Connection is connected
     */
    boolean isConnected();

    /**
     * @return true if the Connection is active
     */
    boolean isActive();

    /**
     * Returns the number of messages to be dispatched to this connection
     */
    int getDispatchQueueSize();
    
    /**
     * Resets the statistics
     */
    void resetStatistics();

    /**
     * Returns the number of messages enqueued on this connection
     * 
     * @return the number of messages enqueued on this connection
     */
    long getEnqueueCount();

    /**
     * Returns the number of messages dequeued on this connection
     * 
     * @return the number of messages dequeued on this connection
     */
    long getDequeueCount();
    
    /**
     * Returns the source address for this connection
     * 
     * @return the souce address for this connection
     */
    String getRemoteAddress();

}
