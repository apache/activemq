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

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTestSupport;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.memory.MemoryPersistenceAdapter;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.usage.SystemUsage;

public class NetworkTestSupport extends BrokerTestSupport {

    protected ArrayList connections = new ArrayList();

    protected TransportConnector connector;

    protected PersistenceAdapter remotePersistenceAdapter;
    protected BrokerService remoteBroker;
    protected SystemUsage remoteMemoryManager;
    protected TransportConnector remoteConnector;

    protected void setUp() throws Exception {

        super.setUp();
        connector = createConnector();
        connector.start();

        remotePersistenceAdapter = createRemotePersistenceAdapter(true);
        remotePersistenceAdapter.start();
        remoteBroker = createRemoteBroker(remotePersistenceAdapter);
        remoteBroker.start();
        BrokerRegistry.getInstance().bind("remotehost", remoteBroker);
        remoteConnector = createRemoteConnector();
        remoteConnector.start();
    }

    /**
     * @return
     * @throws Exception
     * @throws IOException
     * @throws URISyntaxException
     */
    protected TransportConnector createRemoteConnector() throws Exception, IOException, URISyntaxException {
        return new TransportConnector(remoteBroker.getBroker(), TransportFactory.bind(broker.getBrokerName(), new URI(getRemoteURI())));
    }

    /**
     * @param value
     * @return
     * @throws Exception
     * @throws IOException
     * @throws URISyntaxException
     */
    protected TransportConnector createConnector() throws Exception, IOException, URISyntaxException {
        return new TransportConnector(broker.getBroker(), TransportFactory.bind(broker.getBrokerName(), new URI(getLocalURI())));
    }

    protected String getRemoteURI() {
        return "vm://remotehost";
    }

    protected String getLocalURI() {
        return "vm://localhost";
    }

    protected PersistenceAdapter createRemotePersistenceAdapter(boolean clean) throws Exception {
        if (remotePersistenceAdapter == null || clean) {
            remotePersistenceAdapter = new MemoryPersistenceAdapter();
        }
        return remotePersistenceAdapter;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:()/localhost?persistent=false&useJmx=false&"));
        return broker;
    }

    protected BrokerService createRemoteBroker(PersistenceAdapter persistenceAdapter) throws Exception {
        BrokerService answer = new BrokerService();
        answer.setBrokerName("remote");
        answer.setUseJmx(false);
        answer.setPersistenceAdapter(persistenceAdapter);
        return answer;
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

    protected Transport createTransport() throws Exception {
        Transport transport = TransportFactory.connect(connector.getServer().getConnectURI());
        return transport;
    }

    protected Transport createRemoteTransport() throws Exception {
        Transport transport = TransportFactory.connect(remoteConnector.getServer().getConnectURI());
        return transport;
    }

    /**
     * Simulates a broker restart. The memory based persistence adapter is
     * reused so that it does not "loose" it's "persistent" messages.
     * 
     * @throws Exception
     */
    protected void restartRemoteBroker() throws Exception {

        BrokerRegistry.getInstance().unbind("remotehost");
        remoteConnector.stop();

        remoteBroker.stop();
        remotePersistenceAdapter.stop();
        remotePersistenceAdapter = createRemotePersistenceAdapter(false);
        remotePersistenceAdapter.start();
        remoteBroker = createRemoteBroker(remotePersistenceAdapter);
        remoteBroker.start();
        String brokerId = remoteBroker.getBrokerName();
        remoteConnector = new TransportConnector(broker.getBroker(), TransportFactory.bind(brokerId, new URI(getRemoteURI())));
        remoteConnector.start();
        BrokerRegistry.getInstance().bind("remotehost", remoteBroker);
    }

    protected void tearDown() throws Exception {
        for (Iterator iter = connections.iterator(); iter.hasNext();) {
            StubConnection connection = (StubConnection)iter.next();
            connection.stop();
            iter.remove();
        }

        BrokerRegistry.getInstance().unbind("remotehost");
        remoteConnector.stop();
        connector.stop();

        remoteBroker.stop();
        remotePersistenceAdapter.stop();
        super.tearDown();
    }

}
