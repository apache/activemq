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

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Iterator;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.BrokerTest;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.TransportConnector;

public abstract class TransportBrokerTestSupport extends BrokerTest {

    private TransportConnector connector;
    private ArrayList<StubConnection> connections = new ArrayList<StubConnection>();

    protected void setUp() throws Exception {
        super.setUp();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService service = super.createBroker();
        connector = service.addConnector(getBindLocation());
        return service;
    }
    
    protected abstract String getBindLocation();

    protected void tearDown() throws Exception {
        for (Iterator<StubConnection> iter = connections.iterator(); iter.hasNext();) {
            StubConnection connection = iter.next();
            connection.stop();
            iter.remove();
        }
        connector.stop();
        super.tearDown();
    }

    protected URI getBindURI() throws URISyntaxException {
        return new URI(getBindLocation());
    }

    protected StubConnection createConnection() throws Exception {
        URI bindURI = getBindURI();
        
        // Note: on platforms like OS X we cannot bind to the actual hostname, so we
        // instead use the original host name (typically localhost) to bind to 
        
        URI actualURI = connector.getServer().getConnectURI();
        URI connectURI = new URI(actualURI.getScheme(), actualURI.getUserInfo(), bindURI.getHost(), actualURI.getPort(), actualURI.getPath(), actualURI
                .getQuery(), actualURI.getFragment());

        Transport transport = TransportFactory.connect(connectURI);
        StubConnection connection = new StubConnection(transport);
        connections.add(connection);
        return connection;
    }

}
