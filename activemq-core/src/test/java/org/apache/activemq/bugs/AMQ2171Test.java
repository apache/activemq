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

import java.util.*;
import java.util.concurrent.CopyOnWriteArrayList;
import javax.jms.*;
import javax.jms.Queue;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class AMQ2171Test implements Thread.UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ2171Test.class);
    private static final String BROKER_URL = "tcp://localhost:0";
    private static final int QUEUE_SIZE = 100;

    private static BrokerService brokerService;
    private static Queue destination;

    private String brokerUri;
    private String brokerUriNoPrefetch;
    private Collection<Throwable> exceptions = new CopyOnWriteArrayList<Throwable>();

    @Before
    public void setUp() throws Exception {
        // Start an embedded broker up.
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.addConnector(BROKER_URL);
        brokerService.start();

        brokerUri = brokerService.getTransportConnectors().get(0).getPublishableConnectString().toString();
        brokerUriNoPrefetch = brokerUri + "?jms.prefetchPolicy.all=0";

        destination = new ActiveMQQueue("Test");
        produce(brokerUri, QUEUE_SIZE);
    }

    @Before
    public void addHandler() {
        Thread.setDefaultUncaughtExceptionHandler(this);
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
    }

    @Test(timeout = 10000)
    public void testBrowsePrefetch() throws Exception {
        runTest(brokerUri);
    }

    @Test(timeout = 10000)
    public void testBrowseNoPrefetch() throws Exception {
        runTest(brokerUriNoPrefetch);
    }

    private void runTest(String brokerURL) throws Exception {

        Connection connection = new ActiveMQConnectionFactory(brokerURL).createConnection();

        try {
            connection.start();

            Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
            @SuppressWarnings("unchecked")
            Enumeration<Message> unread = (Enumeration<Message>) session.createBrowser(destination).getEnumeration();

            int count = 0;
            while (unread.hasMoreElements()) {
                unread.nextElement();
                count++;
            }

            assertEquals(QUEUE_SIZE, count);
            assertTrue(exceptions.isEmpty());
        } finally {
            try {
                connection.close();
            } catch (JMSException e) {
                exceptions.add(e);
            }
        }
    }

    private static void produce(String brokerURL, int count) throws Exception {
        Connection connection = null;

        try {

            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerURL);
            connection = factory.createConnection();
            Session session = connection.createSession(false,Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(destination);
            producer.setTimeToLive(0);
            connection.start();

            for (int i = 0; i < count; i++) {
                int id = i + 1;
                TextMessage message = session.createTextMessage("Message " + id);
                message.setIntProperty("MsgNumber", id);
                producer.send(message);

                if (id % 500 == 0) {
                    LOG.info("sent " + id + ", ith " + message);
                }
            }
        } finally {
            try {
                if (connection != null) {
                    connection.close();
                }
            } catch (Throwable e) {
            }
        }
    }

    public void uncaughtException(Thread t, Throwable e) {
        exceptions.add(e);
    }
}
