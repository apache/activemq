/*
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
package org.apache.activemq.transport.amqp.client.transport;

import java.net.URI;
import java.util.Map;

import org.apache.activemq.transport.amqp.client.util.PropertyUtil;

/**
 * Factory for creating the Netty based TCP Transport.
 */
public final class NettyTransportFactory {

    private NettyTransportFactory() {}

    /**
     * Creates an instance of the given Transport and configures it using the
     * properties set on the given remote broker URI.
     *
     * @param remoteURI
     *        The URI used to connect to a remote Peer.
     *
     * @return a new Transport instance.
     *
     * @throws Exception if an error occurs while creating the Transport instance.
     */
    public static NettyTransport createTransport(URI remoteURI) throws Exception {
        Map<String, String> map = PropertyUtil.parseQuery(remoteURI.getQuery());
        Map<String, String> transportURIOptions = PropertyUtil.filterProperties(map, "transport.");
        NettyTransportOptions transportOptions = null;

        remoteURI = PropertyUtil.replaceQuery(remoteURI, map);

        if (!remoteURI.getScheme().equalsIgnoreCase("ssl") && !remoteURI.getScheme().equalsIgnoreCase("wss")) {
            transportOptions = NettyTransportOptions.INSTANCE.clone();
        } else {
            transportOptions = NettyTransportSslOptions.INSTANCE.clone();
        }

        Map<String, String> unused = PropertyUtil.setProperties(transportOptions, transportURIOptions);
        if (!unused.isEmpty()) {
            String msg = " Not all transport options could be set on the TCP based" +
                         " Transport. Check the options are spelled correctly." +
                         " Unused parameters=[" + unused + "]." +
                         " This provider instance cannot be started.";
            throw new IllegalArgumentException(msg);
        }

        NettyTransport result = null;

        switch (remoteURI.getScheme().toLowerCase()) {
            case "tcp":
            case "ssl":
                result = new NettyTcpTransport(remoteURI, transportOptions);
                break;
            case "ws":
            case "wss":
                result = new NettyWSTransport(remoteURI, transportOptions);
                break;
            default:
                throw new IllegalArgumentException("Invalid URI Scheme: " + remoteURI.getScheme());
        }

        return result;
    }
}
