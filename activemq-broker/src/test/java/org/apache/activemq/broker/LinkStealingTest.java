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
package org.apache.activemq.broker;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import javax.jms.Connection;
import javax.jms.InvalidClientIDException;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ConnectionInfo;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LinkStealingTest {

    private BrokerService brokerService;
    private final AtomicReference<Throwable> removeException = new AtomicReference<Throwable>();

    private String stealableConnectionURI;
    private String unstealableConnectionURI;

    @SuppressWarnings("unchecked")
    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setPlugins(new BrokerPlugin[]{
            new BrokerPluginSupport() {
                @Override
                public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
                    removeException.set(error);
                    super.removeConnection(context, info, error);
                }
            }
        });

        stealableConnectionURI = brokerService.addConnector("tcp://0.0.0.0:0?allowLinkStealing=true").getPublishableConnectString();
        unstealableConnectionURI = brokerService.addConnector("tcp://0.0.0.0:0?allowLinkStealing=false").getPublishableConnectString();

        brokerService.start();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }

    @Test(timeout=60000)
    public void testStealLinkFails() throws Exception {

        final String clientID = "ThisIsAClientId";
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(unstealableConnectionURI);
        Connection connection1 = factory.createConnection();
        connection1.setClientID(clientID);
        connection1.start();

        AtomicBoolean exceptionFlag = new AtomicBoolean();
        try {
            Connection connection2 = factory.createConnection();
            connection2.setClientID(clientID);
            connection2.start();
        } catch (InvalidClientIDException e) {
            exceptionFlag.set(true);
        }

        assertTrue(exceptionFlag.get());
    }

    @Test(timeout=60000)
    public void testStealLinkSuccess() throws Exception {

        final String clientID = "ThisIsAClientId";
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(stealableConnectionURI);
        Connection connection1 = factory.createConnection();
        connection1.setClientID(clientID);
        connection1.start();

        AtomicBoolean exceptionFlag = new AtomicBoolean();
        try {
            Connection connection2 = factory.createConnection();
            connection2.setClientID(clientID);
            connection2.start();
        } catch (InvalidClientIDException e) {
            e.printStackTrace();
            exceptionFlag.set(true);
        }

        assertFalse(exceptionFlag.get());
        assertNotNull(removeException.get());
    }
}
