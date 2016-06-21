/*
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
package org.apache.activemq.transport.amqp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tests interoperability between OpenWire and AMQP
 */
@RunWith(Parameterized.class)
public class JMSInteroperabilityTest extends JMSClientTestSupport {

    protected static final Logger LOG = LoggerFactory.getLogger(JMSInteroperabilityTest.class);

    private final String transformer;

    @Parameters(name="Transformer->{0}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {"jms"},
                {"native"},
                {"raw"},
            });
    }

    public JMSInteroperabilityTest(String transformer) {
        this.transformer = transformer;
    }

    @Override
    protected boolean isUseOpenWireConnector() {
        return true;
    }

    @Override
    protected String getAmqpTransformer() {
        return transformer;
    }

    //----- Tests for OpenWire to Qpid JMS using MapMessage ------------------//

    @SuppressWarnings("unchecked")
    @Test
    public void testMapMessageSendReceive() throws Exception {
        Connection openwire = createJMSConnection();
        Connection amqp = createConnection();

        openwire.start();
        amqp.start();

        Session openwireSession = openwire.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session amqpSession = amqp.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination queue = openwireSession.createQueue(getDestinationName());

        MessageProducer openwireProducer = openwireSession.createProducer(queue);
        MessageConsumer amqpConsumer = amqpSession.createConsumer(queue);

        // Create the Message
        ObjectMessage outgoing = openwireSession.createObjectMessage();

        HashMap<String, Object> outgoingMap = new HashMap<String, Object>();

        outgoingMap.put("none", null);
        outgoingMap.put("string", "test");
        outgoingMap.put("long", 255L);
        outgoingMap.put("empty-string", "");
        outgoingMap.put("negative-int", -1);
        outgoingMap.put("float", 0.12f);

        outgoing.setObject(outgoingMap);

        openwireProducer.send(outgoing);

        // Now consumer the ObjectMessage
        Message received = amqpConsumer.receive(2000);
        assertNotNull(received);
        assertTrue(received instanceof ObjectMessage);
        ObjectMessage incoming = (ObjectMessage) received;

        Object incomingObject = incoming.getObject();
        assertNotNull(incomingObject);
        assertTrue(incomingObject instanceof Map);
        Map<String, Object> incomingMap = (Map<String, Object>) incomingObject;
        assertEquals(outgoingMap.size(), incomingMap.size());

        amqp.close();
        openwire.close();
    }

    //----- Tests for OpenWire to Qpid JMS using ObjectMessage ---------------//

    @SuppressWarnings("unchecked")
    @Test
    public void testObjectMessageContainingList() throws Exception {
        Connection openwire = createJMSConnection();
        Connection amqp = createConnection();

        openwire.start();
        amqp.start();

        Session openwireSession = openwire.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session amqpSession = amqp.createSession(false, Session.AUTO_ACKNOWLEDGE);

        Destination queue = openwireSession.createQueue(getDestinationName());

        MessageProducer openwireProducer = openwireSession.createProducer(queue);
        MessageConsumer amqpConsumer = amqpSession.createConsumer(queue);

        // Create the Message
        ObjectMessage outgoing = openwireSession.createObjectMessage();

        ArrayList<Object> outgoingList = new ArrayList<Object>();

        outgoingList.add(null);
        outgoingList.add("test");
        outgoingList.add(255L);
        outgoingList.add("");
        outgoingList.add(-1);
        outgoingList.add(0.12f);

        outgoing.setObject(outgoingList);

        openwireProducer.send(outgoing);

        // Now consumer the ObjectMessage
        Message received = amqpConsumer.receive(2000);
        assertNotNull(received);
        assertTrue(received instanceof ObjectMessage);
        ObjectMessage incoming = (ObjectMessage) received;

        Object incomingObject = incoming.getObject();
        assertNotNull(incomingObject);
        assertTrue(incomingObject instanceof List);
        List<Object> incomingList = (List<Object>) incomingObject;
        assertEquals(outgoingList.size(), incomingList.size());

        amqp.close();
        openwire.close();
    }
}
