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
package org.apache.activemq.transport.failover;

import java.io.IOException;
import java.util.Date;
import java.util.concurrent.atomic.AtomicInteger;
import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.transport.TransportListener;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static org.junit.Assert.*;

public class InitalReconnectDelayTest {

    private static final transient Logger LOG = LoggerFactory.getLogger(InitalReconnectDelayTest.class);
    protected BrokerService broker1;
    protected BrokerService broker2;

    @Test
    public void testInitialReconnectDelay() throws Exception {

        String uriString = "failover://(tcp://localhost:" +
            broker1.getTransportConnectors().get(0).getConnectUri().getPort() +
            ",tcp://localhost:" +
            broker2.getTransportConnectors().get(0).getConnectUri().getPort() +
            ")?randomize=false&initialReconnectDelay=15000";

        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(uriString);
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue("foo");
        MessageProducer producer = session.createProducer(destination);

        long start = (new Date()).getTime();
        producer.send(session.createTextMessage("TEST"));
        long end = (new Date()).getTime();

        //Verify we can send quickly
        assertTrue((end - start) < 2000);

        //Halt the broker1...
        LOG.info("Stopping the Broker1...");
        start = (new Date()).getTime();
        broker1.stop();

        LOG.info("Attempting to send... failover should kick in...");
        producer.send(session.createTextMessage("TEST"));
        end = (new Date()).getTime();

        //Inital reconnection should kick in and be darned close to what we expected
        LOG.info("Failover took " + (end - start) + " ms.");
        assertTrue("Failover took " + (end - start) + " ms and should be > 14000.", (end - start) > 14000);
    }

    @Test
    public void testNoSuspendedCallbackOnNoReconnect() throws Exception {

        String uriString = "failover://(tcp://localhost:" +
            broker1.getTransportConnectors().get(0).getConnectUri().getPort() +
            ",tcp://localhost:" +
            broker2.getTransportConnectors().get(0).getConnectUri().getPort() +
            ")?randomize=false&maxReconnectAttempts=0";


        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(uriString);
        final AtomicInteger calls = new AtomicInteger(0);
        connectionFactory.setTransportListener(new TransportListener() {
            @Override
            public void onCommand(Object command) {
            }

            @Override
            public void onException(IOException error) {
                LOG.info("on exception: " + error);
                calls.set(0x01 | calls.intValue());
            }

            @Override
            public void transportInterupted() {
                LOG.info("on transportInterupted");
                calls.set(0x02 | calls.intValue());
            }

            @Override
            public void transportResumed() {
                LOG.info("on transportResumed");
                calls.set(0x04 | calls.intValue());
            }
        });
        Connection connection = connectionFactory.createConnection();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue destination = session.createQueue("foo");
        MessageProducer producer = session.createProducer(destination);

        final Message message = session.createTextMessage("TEST");
        producer.send(message);

        // clear listener state
        calls.set(0);

        LOG.info("Stopping the Broker1...");
        broker1.stop();

        LOG.info("Attempting to send... failover should throw on disconnect");
        try {
            producer.send(destination, message);
            fail("Expect IOException to bubble up on send");
        } catch (javax.jms.IllegalStateException producerClosed) {
        }

        assertEquals("Only an exception is reported to the listener", 0x1, calls.get());
    }

    @Before
    public void setUp() throws Exception {

        final String dataDir = "target/data/shared";

        broker1 = new BrokerService();

        broker1.setBrokerName("broker1");
        broker1.setDeleteAllMessagesOnStartup(true);
        broker1.setDataDirectory(dataDir);
        broker1.addConnector("tcp://localhost:0");
        broker1.setUseJmx(false);
        broker1.start();
        broker1.waitUntilStarted();

        broker2 = new BrokerService();
        broker2.setBrokerName("broker2");
        broker2.setDataDirectory(dataDir);
        broker2.setUseJmx(false);
        broker2.addConnector("tcp://localhost:0");
        broker2.start();
        broker2.waitUntilStarted();

    }

    protected String getSlaveXml() {
        return "org/apache/activemq/broker/ft/sharedFileSlave.xml";
    }

    protected String getMasterXml() {
        return "org/apache/activemq/broker/ft/sharedFileMaster.xml";
    }

    @After
    public void tearDown() throws Exception {

        if (broker1.isStarted()) {
            broker1.stop();
            broker1.waitUntilStopped();
        }

        if (broker2.isStarted()) {
            broker2.stop();
            broker2.waitUntilStopped();
        }
    }

}
