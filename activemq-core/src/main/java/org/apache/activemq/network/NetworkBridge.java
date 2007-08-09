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
package org.apache.activemq.network;

import org.apache.activemq.Service;


/**
 * Represents a network bridge interface
 * 
 * @version $Revision: 1.1 $
 */
public interface NetworkBridge extends Service {
    
    /**
     * Service an exception
     * @param error
     */
    void serviceRemoteException(Throwable error);
    
    /**
     * servicee an exception
     * @param error
     */
    void serviceLocalException(Throwable error);
    
    /**
     * Set the NetworkBridgeFailedListener
     * @param listener
     */
    void setNetworkBridgeListener(NetworkBridgeListener listener);
    
    
    String getRemoteAddress();

    String getRemoteBrokerName();

    String getLocalAddress();

    String getLocalBrokerName();

    long getEnqueueCounter();

    long getDequeueCounter();
}
