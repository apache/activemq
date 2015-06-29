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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.BeforeClass;

import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.log4j.Appender;
import org.apache.log4j.spi.LoggingEvent;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.AbortSlowConsumerStrategy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.transport.failover.FailoverTransport;
import org.apache.activemq.util.DefaultIOExceptionHandler;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.Wait;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.TransactionRolledBackException;
import java.io.IOException;

/**
 * The FailoverTransport maintains state is the ConnectionStateTracker object. Aborted slow consumers must be removed
 * from this state tracker so that the FailoverTransport does not re-register the aborted slow consumers.
 */
public class AMQ5844Test {

    static final Logger LOG = LoggerFactory.getLogger(AMQ5844Test.class);

    protected BrokerService broker;

    protected long checkPeriod = 2 * 1000;
    protected long maxSlowDuration = 4 * 1000;

    private String uri;

    private final static String QUEUE_NAME = "TEST.QUEUE";

    static boolean abortingSlowConsumer = false;
    static boolean successfullyReconnected = false;

    static final Appender appender = new DefaultTestAppender() {
        @Override
        public void doAppend(LoggingEvent event) {
            if(event.getMessage().toString().contains("aborting slow consumer")) {
                abortingSlowConsumer = true;
            }

            if(event.getMessage().toString().contains("Successfully reconnected to")) {
                successfullyReconnected = true;
            }
        }
    };

    @BeforeClass
    public static void setUp() throws Exception {
        org.apache.log4j.Logger.getRootLogger().addAppender(appender);
    }


    @Before
    /**
     * Sets a AbortSlowConsumerStrategy policy entry on the broker and starts the broker.
     */
    public void createMaster() throws Exception{
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        TransportConnector transportConnector = broker.addConnector("tcp://0.0.0.0:0");

        DefaultIOExceptionHandler defaultIOExceptionHandler = new DefaultIOExceptionHandler();
        broker.setIoExceptionHandler(defaultIOExceptionHandler);
        broker.setBrokerName("Main");

        PolicyEntry policy = new PolicyEntry();
        AbortSlowConsumerStrategy abortSlowConsumerStrategy = new AbortSlowConsumerStrategy();
        abortSlowConsumerStrategy.setAbortConnection(false);
        //time in milliseconds between checks for slow subscriptions
        abortSlowConsumerStrategy.setCheckPeriod(checkPeriod);
        //time in milliseconds that a sub can remain slow before triggering an abort
        abortSlowConsumerStrategy.setMaxSlowDuration(maxSlowDuration);

        policy.setSlowConsumerStrategy(abortSlowConsumerStrategy);
        policy.setQueuePrefetch(0);
        PolicyMap pMap = new PolicyMap();
        pMap.setDefaultEntry(policy);
        broker.setDestinationPolicy(pMap);

        broker.start();
        uri = transportConnector.getPublishableConnectString();
    }

    @After
    public void stopBroker() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
        org.apache.log4j.Logger.getRootLogger().removeAppender(appender);
    }

    @Test
    public void testRecreateAbortedConsumer() throws Exception {
        String failoverTransportURL = "failover:(" + uri + ")";

        ActiveMQConnectionFactory amq = new ActiveMQConnectionFactory(failoverTransportURL);
        amq.setWatchTopicAdvisories(false);

        Connection jmsConnection = amq.createConnection();

        ActiveMQConnection connection = (ActiveMQConnection) jmsConnection;

        connection.start();

        // Create a Session that is transacted
        Session session = connection.createSession(true, Session.SESSION_TRANSACTED);

        //Create the destination Queue
        Destination destination = session.createQueue(QUEUE_NAME);

        //Create a MessageProducer from the Session to the Queue
        MessageProducer producer = session.createProducer(destination);

        // Create message, send and close producer
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("Plain Text Message");

        String text = stringBuilder.toString();
        TextMessage message = session.createTextMessage(text);

        //Send 2 non-persistent text messages
        producer.send(message, DeliveryMode.NON_PERSISTENT, 1, 0);
        producer.send(message, DeliveryMode.NON_PERSISTENT, 1, 0);
        //Commit the session so the messages get delivered to the broker
        session.commit();
        //close the producer and get it out of the way
        producer.close();

        //Start consuming the messages.
        ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(destination);
        Message mess = consumer.receive();
        //First message received successfully.
        assertNotNull(mess);

        //The AbortSlowConsumerStrategy kicks in here and sends a message down to the client to close itself.
        //The client does not close because it is in the middle of the transaction. Meanwhile the FailoverTransport
        //detects the close command and removes the consumer from its state.

        assertTrue("The browser aborts the slow consumer", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return abortingSlowConsumer;
            }
        }, 10 * 1000));

        //We intentionally fail the transport just to make sure that the slow consumer that was aborted is not
        //re-registered by the FailoverTransport
        FailoverTransport failoverTransport = connection.getTransport().narrow(FailoverTransport.class);
        failoverTransport.handleTransportFailure(new IOException());

        assertTrue("The broker aborts the slow consumer", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return successfullyReconnected;
            }
        }, 4 * 1000));

        try {
            mess = consumer.receive(2 * 1000);
            //This message must be null because the slow consumer has already been deleted on the broker side.
            assertNull(mess);
            session.commit();
            fail("Expect the commit to fail and a rollback to happen");
        }
        catch (TransactionRolledBackException expected) {
            assertTrue(expected.getMessage().contains("rolling back transaction"));
        }

        connection.close();
    }
}
