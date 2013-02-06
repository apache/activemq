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
package org.apache.activemq.transport;

import java.net.InetAddress;
import java.net.URI;

import org.apache.activemq.util.InetAddressUtil;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;

abstract public class WebTransportServerSupport extends TransportServerSupport {

    protected URI bindAddress;
    protected Server server;
    protected Connector connector;
    protected SocketConnectorFactory socketConnectorFactory;
    protected String host;

    public WebTransportServerSupport(URI location) {
        super(location);
    }

    public URI bind() throws Exception {

        URI bind = getBindLocation();

        String bindHost = bind.getHost();
        bindHost = (bindHost == null || bindHost.length() == 0) ? "localhost" : bindHost;
        InetAddress addr = InetAddress.getByName(bindHost);
        host = addr.getCanonicalHostName();

        connector.setHost(host);
        connector.setPort(bindAddress.getPort());
        connector.setServer(server);
        server.addConnector(connector);
        if (addr.isAnyLocalAddress()) {
            host = InetAddressUtil.getLocalHostName();
        }

        URI boundUri = new URI(bind.getScheme(), bind.getUserInfo(), host, bindAddress.getPort(), bind.getPath(), bind.getQuery(), bind.getFragment());
        setConnectURI(boundUri);
        return boundUri;
    }
}
