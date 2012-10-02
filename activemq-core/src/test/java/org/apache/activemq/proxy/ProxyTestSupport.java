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
package org.apache.activemq.proxy;

import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.usage.SystemUsage;

public class ProxyTestSupport extends BrokerTestSupport {

    protected ArrayList<StubConnection> connections = new ArrayList<StubConnection>();

    protected TransportConnector connector;

    protected PersistenceAdapter remotePersistenceAdapter;
    protected BrokerService remoteBroker;
    protected SystemUsage remoteMemoryManager;
    protected TransportConnector remoteConnector;
    private ProxyConnector proxyConnector;
    private ProxyConnector remoteProxyConnector;

    protected BrokerService createBroker() throws Exception {
        BrokerService service = new BrokerService();
        service.setBrokerName("broker1");
        service.setPersistent(false);
        service.setUseJmx(false);

        connector = service.addConnector(getLocalURI());
        proxyConnector = new ProxyConnector();
        proxyConnector.setName("proxy");
        proxyConnector.setBind(new URI(getLocalProxyURI()));
        proxyConnector.setRemote(new URI("fanout:static://" + getRemoteURI()));
        service.addProxyConnector(proxyConnector);

        return service;
    }

    protected BrokerService createRemoteBroker() throws Exception {
        BrokerService service = new BrokerService();
        service.setBrokerName("broker2");
        service.setPersistent(false);
        service.setUseJmx(false);

        remoteConnector = service.addConnector(getRemoteURI());
        remoteProxyConnector = new ProxyConnector();
        remoteProxyConnector.setName("remoteProxy");
        remoteProxyConnector.setBind(new URI(getRemoteProxyURI()));
        remoteProxyConnector.setRemote(new URI("fanout:static://" + getLocalURI()));
        service.addProxyConnector(remoteProxyConnector);

        return service;
    }

    protected void setUp() throws Exception {
        super.setUp();
        remoteBroker = createRemoteBroker();
        remoteBroker.start();
    }

    protected void tearDown() throws Exception {
        for (Iterator<StubConnection> iter = connections.iterator(); iter.hasNext();) {
            StubConnection connection = iter.next();
            connection.stop();
            iter.remove();
        }
        remoteBroker.stop();
        super.tearDown();
    }

    protected String getRemoteURI() {
        return "tcp://localhost:6171";
    }

    protected String getLocalURI() {
        return "tcp://localhost:6161";
    }

    protected String getRemoteProxyURI() {
        return "tcp://localhost:6162";
    }

    protected String getLocalProxyURI() {
        return "tcp://localhost:6172";
    }

    protected StubConnection createConnection() throws Exception {
        Transport transport = TransportFactory.connect(connector.getServer().getConnectURI());
        StubConnection connection = new StubConnection(transport);
        connections.add(connection);
        return connection;
    }

    protected StubConnection createRemoteConnection() throws Exception {
        Transport transport = TransportFactory.connect(remoteConnector.getServer().getConnectURI());
        StubConnection connection = new StubConnection(transport);
        connections.add(connection);
        return connection;
    }

    protected StubConnection createProxyConnection() throws Exception {
        Transport transport = TransportFactory.connect(proxyConnector.getServer().getConnectURI());
        StubConnection connection = new StubConnection(transport);
        connections.add(connection);
        return connection;
    }

    protected StubConnection createRemoteProxyConnection() throws Exception {
        Transport transport = TransportFactory.connect(remoteProxyConnector.getServer().getConnectURI());
        StubConnection connection = new StubConnection(transport);
        connections.add(connection);
        return connection;
    }

}
