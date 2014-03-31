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
package org.apache.activemq.transport.discovery.http;

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.TransportListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HttpDiscoveryTest implements TransportListener {

    private BrokerService broker;
    private ActiveMQConnectionFactory factory;
    private final CountDownLatch discovered = new CountDownLatch(1);

    @Before
    public void setUp() throws Exception {

        broker = new BrokerService();
        TransportConnector connector = broker.addConnector("tcp://localhost:0");
        connector.setDiscoveryUri(new URI("http://localhost:8181/default?startEmbeddRegistry=true"));
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.deleteAllMessages();
        broker.start();

        String connectionUri = "discovery:http://localhost:8181/default";
        factory = new ActiveMQConnectionFactory(connectionUri + "?trace=true&soTimeout=1000");
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test
    public void testBrokerIsDiscovered() throws Exception {
        factory.setTransportListener(this);
        ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        assertTrue(discovered.await(60, TimeUnit.SECONDS));
        connection.close();
    }

    @Override
    public void onCommand(Object command) {
    }

    @Override
    public void onException(IOException error) {
    }

    @Override
    public void transportInterupted() {
    }

    @Override
    public void transportResumed() {
        discovered.countDown();
    }

}
