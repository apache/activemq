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

import org.apache.activemq.transport.Transport;

/**
 * Factory for network bridges
 * 
 * @version $Revision: 1.1 $
 */
public final class NetworkBridgeFactory {

    private NetworkBridgeFactory() {
    }
    
    /**
     * Create a network bridge
     * 
     * @param config
     * @param localTransport
     * @param remoteTransport
     * @return the NetworkBridge
     */
    public static DemandForwardingBridge createBridge(NetworkBridgeConfiguration config,
                                                      Transport localTransport, Transport remoteTransport) {
        return createBridge(config, localTransport, remoteTransport, null);
    }

    /**
     * create a network bridge
     * 
     * @param configuration
     * @param localTransport
     * @param remoteTransport
     * @param listener
     * @return the NetworkBridge
     */
    public static DemandForwardingBridge createBridge(NetworkBridgeConfiguration configuration,
                                                      Transport localTransport, Transport remoteTransport,
                                                      final NetworkBridgeListener listener) {
        DemandForwardingBridge result = null;
        if (configuration.isConduitSubscriptions()) {
            if (configuration.isDynamicOnly()) {
                result = new ConduitBridge(configuration, localTransport, remoteTransport);
            } else {
                result = new DurableConduitBridge(configuration, localTransport, remoteTransport);
            }
        } else {
            result = new DemandForwardingBridge(configuration, localTransport, remoteTransport);
        }
        if (listener != null) {
            result.setNetworkBridgeListener(listener);
        }
        return result;
    }
}
