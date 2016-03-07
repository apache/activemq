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
package org.apache.activemq.bugs;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Collection;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQDestination;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

/**
 * Test that statistics for a sent message are properly counted for a temporary destination
 * whether inside a transaction or not.
 */
@RunWith(Parameterized.class)
public class AMQ6194Test {

    private boolean transaction;

    @Parameters(name = "transaction:{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
            //!transaction
            {false},
            //transaction
            {true}
        });
    }

    private BrokerService brokerService;
    private String connectionUri;

    /**
     * @param transaction
     */
    public AMQ6194Test(boolean transaction) {
        super();
        this.transaction = transaction;
    }

    @Before
    public void before() throws Exception {
        brokerService = new BrokerService();
        TransportConnector connector = brokerService.addConnector("tcp://localhost:0");
        connectionUri = connector.getPublishableConnectString();
        brokerService.setPersistent(false);
        brokerService.start();
        brokerService.waitUntilStarted();
    }

    @After
    public void after() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Test
    public void testTempStatistics() throws Exception {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        Connection connection = null;
        Session session = null;
        MessageProducer producer = null;
        TemporaryQueue temporaryQueue = null;
        try {
            connection = factory.createConnection();
            connection.start();
            if (transaction) {
                session = connection.createSession(true, Session.SESSION_TRANSACTED);
            } else {
                session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            }
            temporaryQueue = session.createTemporaryQueue();
            producer = session.createProducer(temporaryQueue);
            final TextMessage textMessage = session.createTextMessage();
            textMessage.setText("Text Message");

            producer.send(textMessage);

            if (transaction) {
                session.commit();
            }
            Destination dest = brokerService.getDestination((ActiveMQDestination) temporaryQueue);
            assertEquals(1, dest.getDestinationStatistics().getMessages().getCount());
        } finally {
            if (connection != null) {
                connection.close();
            }
        }
    }
}