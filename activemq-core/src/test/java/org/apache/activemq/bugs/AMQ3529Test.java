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

import static org.junit.Assert.*;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class AMQ3529Test {

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private Session session;
    private BrokerService broker;
    private String connectionUri;

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.addConnector("tcp://0.0.0.0:0");
        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();

        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout = 60000)
    public void testInterruptionAffects() throws Exception {
        ThreadGroup tg = new ThreadGroup("tg");

        assertEquals(0, tg.activeCount());

        Thread client = new Thread(tg, "client") {

            @Override
            public void run() {
                try {
                    connection = connectionFactory.createConnection();
                    session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
                    assertNotNull(session);
                } catch (JMSException e) {
                    fail(e.getMessage());
                }
                // next line is the nature of the test, if I remove this line, everything works OK
                Thread.currentThread().interrupt();
                try {
                    connection.close();
                } catch (JMSException e) {
                }

                assertTrue(Thread.currentThread().isInterrupted());
            }
        };
        client.start();
        client.join();
        Thread.sleep(2000);
        Thread[] remainThreads = new Thread[tg.activeCount()];
        tg.enumerate(remainThreads);
        for (Thread t : remainThreads) {
            if (t.isAlive() && !t.isDaemon())
                fail("Remaining thread: " + t.toString());
        }
    }
}
