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

import javax.management.ObjectName;

import org.apache.activemq.Service;

/**
 * Represents a network bridge interface
 */
public interface NetworkBridge extends Service {

    /**
     * Service an exception received from the Remote Broker connection.
     * @param error
     */
    void serviceRemoteException(Throwable error);

    /**
     * Service an exception received from the Local Broker connection.
     * @param error
     */
    void serviceLocalException(Throwable error);

    /**
     * Set the NetworkBridgeFailedListener
     * @param listener
     */
    void setNetworkBridgeListener(NetworkBridgeListener listener);

    /**
     * @return the network address of the remote broker connection.
     */
    String getRemoteAddress();

    /**
     * @return the name of the remote broker this bridge is connected to.
     */
    String getRemoteBrokerName();

    /**
     * @return the id of the remote broker this bridge is connected to.
     */
    String getRemoteBrokerId();

    /**
     * @return the network address of the local broker connection.
     */
    String getLocalAddress();

    /**
     * @return the name of the local broker this bridge is connected to.
     */
    String getLocalBrokerName();

    /**
     * @return the current number of enqueues this bridge has.
     */
    long getEnqueueCounter();

    /**
     * @return the current number of dequeues this bridge has.
     */
    long getDequeueCounter();

    /**
     * @return the statistics for this NetworkBridge
     */
    NetworkBridgeStatistics getNetworkBridgeStatistics();

    /**
     * @param objectName
     *      The ObjectName assigned to this bridge in the MBean server.
     */
    void setMbeanObjectName(ObjectName objectName);

    /**
     * @return the MBean name used to identify this bridge in the MBean server.
     */
    ObjectName getMbeanObjectName();

    void resetStats();
}
