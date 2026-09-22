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
package org.apache.activemq;

import static org.junit.Assert.*;

import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.Session;

import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.transport.Transport;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * JMS draws a hard line between an <em>identified</em> producer (created with a
 * destination) and an <em>unidentified</em> one (created without):
 *
 * <ul>
 *   <li>the destination-less {@code send(Message, ...)} methods are only valid on an
 *       identified producer;</li>
 *   <li>the destination-taking {@code send(Destination, Message, ...)} methods are only
 *       valid on an unidentified producer.</li>
 * </ul>
 *
 * Misusing either raises {@code UnsupportedOperationException}, per the
 * {@code jakarta.jms.MessageProducer} javadoc. A closed producer still reports
 * {@code IllegalStateException} first, regardless of which form is used.
 *
 * <p>These are client-side guards, so the tests run against a stub transport rather
 * than a broker.
 */
public class ActiveMQMessageProducerIdentifiedTest {

    private StubTransport transport;
    private Connection connection;
    private Session session;
    private Queue queue;

    @Before
    public void setUp() throws Exception {
        transport = new StubTransport();
        ActiveMQConnectionFactory factory =
                new ActiveMQConnectionFactory("tcp://localhost:61616") {
                    @Override
                    protected Transport createTransport() {
                        return transport;
                    }
                };
        // The rules are opt-in: ActiveMQ accepts either send form by default, so the
        // guards only apply under strict compliance.
        factory.setStrictCompliance(true);
        connection = factory.createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        queue = session.createQueue("TEST.QUEUE");
    }

    /** A default (non-strict) connection keeps the long-standing permissive behaviour. */
    @Test
    public void testNonStrictConnectionStillAcceptsEitherForm() throws Exception {
        StubTransport lenientTransport = new StubTransport();
        ActiveMQConnectionFactory lenient =
                new ActiveMQConnectionFactory("tcp://localhost:61616") {
                    @Override
                    protected Transport createTransport() {
                        return lenientTransport;
                    }
                };
        // strictCompliance defaults to false
        Connection c = lenient.createConnection();
        try {
            Session s = c.createSession(false, Session.AUTO_ACKNOWLEDGE);
            Queue q = s.createQueue("TEST.QUEUE");
            MessageProducer identified = s.createProducer(q);
            // Illegal per spec, but accepted for backwards compatibility.
            identified.send(q, s.createTextMessage("legacy"));
        } finally {
            c.close();
        }
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
    }

    // --- identified producer: destination-less sends are the valid form ---

    @Test
    public void testIdentifiedProducerSendMessageIsAllowed() throws Exception {
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("ok"));
        assertTrue("message should have reached the transport", sentAMessage());
    }

    @Test
    public void testIdentifiedProducerSendWithOptionsIsAllowed() throws Exception {
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("ok"), DeliveryMode.NON_PERSISTENT, 4, 0L);
        assertTrue(sentAMessage());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIdentifiedProducerRejectsExplicitDestination() throws Exception {
        MessageProducer producer = session.createProducer(queue);
        producer.send(queue, session.createTextMessage("boom"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testIdentifiedProducerRejectsExplicitDestinationWithOptions() throws Exception {
        MessageProducer producer = session.createProducer(queue);
        producer.send(queue, session.createTextMessage("boom"), DeliveryMode.NON_PERSISTENT, 4, 0L);
    }

    /**
     * The rule is about which method was called, not about the value -- supplying the
     * producer's own destination is still an explicit destination and still rejected.
     */
    @Test(expected = UnsupportedOperationException.class)
    public void testIdentifiedProducerRejectsEvenItsOwnDestination() throws Exception {
        MessageProducer producer = session.createProducer(queue);
        Queue same = session.createQueue("TEST.QUEUE");
        producer.send(same, session.createTextMessage("boom"));
    }

    // --- unidentified producer: destination-taking sends are the valid form ---

    @Test
    public void testUnidentifiedProducerSendWithDestinationIsAllowed() throws Exception {
        MessageProducer producer = session.createProducer(null);
        producer.send(queue, session.createTextMessage("ok"));
        assertTrue("message should have reached the transport", sentAMessage());
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnidentifiedProducerRejectsSendMessage() throws Exception {
        MessageProducer producer = session.createProducer(null);
        producer.send(session.createTextMessage("boom"));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testUnidentifiedProducerRejectsSendMessageWithOptions() throws Exception {
        MessageProducer producer = session.createProducer(null);
        producer.send(session.createTextMessage("boom"), DeliveryMode.NON_PERSISTENT, 4, 0L);
    }

    // --- a closed producer reports IllegalStateException before either rule ---

    @Test(expected = jakarta.jms.IllegalStateException.class)
    public void testClosedIdentifiedProducerReportsIllegalStateNotUnsupported() throws Exception {
        MessageProducer producer = session.createProducer(queue);
        producer.close();
        // Would otherwise be UnsupportedOperationException; closed wins.
        producer.send(queue, session.createTextMessage("boom"));
    }

    @Test(expected = jakarta.jms.IllegalStateException.class)
    public void testClosedUnidentifiedProducerReportsIllegalStateNotUnsupported() throws Exception {
        MessageProducer producer = session.createProducer(null);
        producer.close();
        producer.send(session.createTextMessage("boom"));
    }

    private boolean sentAMessage() {
        for (Object cmd : transport.getSent()) {
            if (cmd instanceof ActiveMQMessage) {
                return true;
            }
        }
        return false;
    }
}
