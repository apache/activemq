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

import junit.framework.TestCase;
import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.Connection;
import javax.jms.InvalidClientIDException;
import java.util.concurrent.atomic.AtomicBoolean;

public class LinkStealingTest extends TestCase {
    protected BrokerService brokerService;
    protected int timeOutInSeconds = 10;


    @Override
    protected void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
    }

    @Override
    protected void tearDown() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
        }
    }


    public void testStealLinkFails() throws Exception {

        brokerService.addConnector(ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL);
        brokerService.start();

        final String clientID = "ThisIsAClientId";
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL);
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

    public void testStealLinkSuccess() throws Exception {

        brokerService.addConnector(ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL+"?allowLinkStealing=true");
        brokerService.start();

        final String clientID = "ThisIsAClientId";
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL);
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

    }
}
