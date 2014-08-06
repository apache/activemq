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
package org.apache.activemq.broker.region;

import java.io.File;

import static org.junit.matchers.JUnitMatchers.containsString;
import static org.junit.Assert.*;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.broker.BrokerService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.junit.*;

/**
 * Confirm that the broker does not resend unacknowledged messages during a broker shutdown.
 */
public class QueueResendDuringShutdownTest {
    private static final Logger         LOG = LoggerFactory.getLogger(QueueResendDuringShutdownTest.class);
    public static final int             NUM_CONNECTION_TO_TEST = 8;

    private static boolean              iterationFoundFailure = false;

    private BrokerService               broker;
    private ActiveMQConnectionFactory   factory;
    private Connection[]                connections;
    private Connection                  producerConnection;
    private Queue                       queue;

    private Object                      messageReceiveSync = new Object();
    private int                         receiveCount;

    @Before
    public void setUp () throws Exception {
        this.receiveCount = 0;

        this.broker = new BrokerService();
        this.broker.setPersistent(false);
        this.broker.start();
        this.broker.waitUntilStarted();

        this.factory = new ActiveMQConnectionFactory(broker.getVmConnectorURI());
        this.queue = new ActiveMQQueue("TESTQUEUE");

        connections = new Connection[NUM_CONNECTION_TO_TEST];
        int iter = 0;
        while ( iter < NUM_CONNECTION_TO_TEST ) {
            this.connections[iter] = factory.createConnection();
            iter++;
        }

        this.producerConnection = factory.createConnection();
        this.producerConnection.start();
    }

    @After
    public void cleanup () throws Exception {
        for ( Connection oneConnection : connections ) {
            if ( oneConnection != null ) {
                closeConnection(oneConnection);
            }
        }
        connections = null;

        if ( this.producerConnection != null ) {
            closeConnection(this.producerConnection);
            this.producerConnection = null;
        }

        this.broker.stop();
        this.broker.waitUntilStopped();
    }

    @Test(timeout=3000)
    public void testRedeliverAtBrokerShutdownAutoAckMsgListenerIter1 () throws Throwable {
        runTestIteration();
    }

    @Test(timeout=3000)
    public void testRedeliverAtBrokerShutdownAutoAckMsgListenerIter2 () throws Throwable {
        runTestIteration();
    }

    @Test(timeout=3000)
    public void testRedeliverAtBrokerShutdownAutoAckMsgListenerIter3 () throws Throwable {
        runTestIteration();
    }

    /**
     * Run one iteration of the test, skipping it if a failure was found on a prior iteration since a single failure is
     * enough.  Also keep track of the state of failure for the iteration.
     */
    protected void  runTestIteration () throws Throwable {
        if ( iterationFoundFailure ) {
            LOG.info("skipping test iteration; failure previously detected");
            return;
        } try {
            testRedeliverAtBrokerShutdownAutoAckMsgListener();
        } catch ( Throwable thrown ) {
            iterationFoundFailure = true;
            throw thrown;
        }
    }

    protected void  testRedeliverAtBrokerShutdownAutoAckMsgListener () throws Exception {
        // Start consumers on all of the connections
        for ( Connection oneConnection : connections ) {
            MessageConsumer consumer = startupConsumer(oneConnection, false, Session.AUTO_ACKNOWLEDGE);
            configureMessageListener(consumer);
            oneConnection.start();
        }

        // Send one message to the Queue and wait a short time for the dispatch to occur.
        this.sendMessage();
        waitForMessage(1000);

        // Verify one consumer received it
        assertEquals(1, this.receiveCount);

        // Shutdown the broker
        this.broker.stop();
        this.broker.waitUntilStopped();
        delay(100, "give queue time flush");

        // Verify still only one consumer received it
        assertEquals(1, this.receiveCount);
    }

    /**
     * Start a consumer on the given connection using the session transaction and acknowledge settings given.
     */
    protected MessageConsumer   startupConsumer (Connection conn, boolean transInd, int ackMode)
    throws JMSException {
        Session         sess;
        MessageConsumer consumer;

        sess = conn.createSession(transInd, ackMode);
        consumer = sess.createConsumer(queue);

        return  consumer;
    }

    /**
     * Mark the receipt of a message from one of the consumers.
     */
    protected void  messageReceived () {
        synchronized ( this ) {
            this.receiveCount++;
            synchronized ( this.messageReceiveSync ) {
                this.messageReceiveSync.notifyAll();
            }
        }
    }

    /**
     * Setup the MessageListener for the given consumer.  The listener uses a long delay on receiving the message to
     * simulate the reported case of problems at shutdown caused by a message listener's connection closing while it is
     * still processing.
     */
    protected void  configureMessageListener (MessageConsumer consumer) throws JMSException {
        final MessageConsumer   fConsumer = consumer;

        consumer.setMessageListener(new MessageListener() {
            public void onMessage (Message msg) {
                LOG.debug("got a message on consumer {}", fConsumer);
                messageReceived();

                // Delay long enough for the consumer to get closed while this delay is active.
                delay(3000, "pause so connection shutdown leads to unacked message redelivery");
            }
        });
    }

    /**
     * Send a test message now.
     */
    protected void  sendMessage () throws JMSException {
        Session sess = this.producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer prod = sess.createProducer(queue);
        prod.send(sess.createTextMessage("X-TEST-MSG-X"));
        prod.close();
        sess.close();
    }

    /**
     * Close the given connection safely and log any exception caught.
     */
    protected void  closeConnection (Connection conn) {
        try {
            conn.close();
        } catch ( JMSException jmsExc ) {
            LOG.info("failed to cleanup connection", jmsExc);
        }
    }

    /**
     * Pause for the given length of time, in milliseconds, logging an interruption if one occurs.  Don't try to
     * recover from interrupt - the test case does not support interrupting and such an occurrence likely means the
     * test is being aborted.
     */
    protected void  delay (long delayMs, String desc) {
        try {
            Thread.sleep(delayMs);
        } catch ( InterruptedException intExc ) {
            LOG.warn("sleep interrupted: " + desc, intExc);
        }
    }

    /**
     * Wait up to the specified duration for a message to be received by any consumer.
     */
    protected void  waitForMessage (long delayMs) {
        try {
            synchronized ( this.messageReceiveSync ) {
                if ( this.receiveCount == 0 ) {
                    this.messageReceiveSync.wait(delayMs);
                }
            }
        } catch ( InterruptedException intExc ) {
            LOG.warn("sleep interrupted: wait for message to arrive");
        }
    }
}
