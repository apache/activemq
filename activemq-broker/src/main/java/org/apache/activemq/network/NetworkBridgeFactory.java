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

import java.net.URI;
import java.util.HashMap;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.util.URISupport;

/**
 * Factory for network bridges
 * 
 * 
 */
public final class NetworkBridgeFactory {

    private NetworkBridgeFactory() {
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
            // dynamicOnly determines whether durables are auto bridged
            result = new DurableConduitBridge(configuration, localTransport, remoteTransport);
        } else {
            result = new DemandForwardingBridge(configuration, localTransport, remoteTransport);
        }
        if (listener != null) {
            result.setNetworkBridgeListener(listener);
        }
        return result;
    }

    public static Transport createLocalTransport(NetworkBridgeConfiguration configuration, URI uri) throws Exception {
        // one end of the localbroker<->bridge transport needs to be async to allow concurrent forwards and acks
        return createLocalTransport(uri, !configuration.isDispatchAsync());
    }

    public static Transport createLocalAsyncTransport(URI uri) throws Exception {
        return createLocalTransport(uri, true);
    }

    private static Transport createLocalTransport(URI uri, boolean async) throws Exception {
        HashMap<String, String> map = new HashMap<String, String>(URISupport.parseParameters(uri));
        map.put("async", String.valueOf(async));
        map.put("create", "false"); // we don't want a vm connect during shutdown to trigger a broker create
        uri = URISupport.createURIWithQuery(uri, URISupport.createQueryString(map));
        return TransportFactory.connect(uri);
    }
}
