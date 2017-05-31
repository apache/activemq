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

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Session;
import javax.jms.Topic;
import org.apache.activemq.broker.BrokerRegistry;
import org.apache.activemq.transport.stomp.Stomp;
import org.apache.activemq.transport.stomp.StompConnection;

public class BrokerDurableRejectedTest extends TestSupport {

    protected Connection connection;
    protected Session consumeSession;
    protected Destination consumerDestination;

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm:(broker:(stomp://localhost:0)?persistent=false&rejectDurableConsumers=true)");
    }

    protected void setUp() throws Exception {
        super.setUp();

        connectionFactory = createConnectionFactory();
        connection = createConnection();

        connection.setClientID(getClass().getName());

        consumeSession = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

        consumerDestination = consumeSession.createTopic("TestDurableRejected");
        connection.start();
    }

    public void testDurableTopicConsumerJms() throws Exception {

        consumeSession.createConsumer(consumerDestination);
        try {

            consumeSession.createDurableSubscriber((Topic)consumerDestination, getName());
            fail("Expect not allowed jms exception on durable creation");

        } catch (JMSException expected) {
            assertTrue("expected exception", expected.getMessage().contains("not allowed"));
        }
    }

    public void testDurableTopicConsumerStomp() throws Exception {

        // verify stomp ok in this case
        StompConnection stompConnection = new StompConnection();
        stompConnection.open("localhost", BrokerRegistry.getInstance().findFirst().getTransportConnectorByScheme("stomp").getPublishableConnectURI().getPort());

        // connect
        String frame = "CONNECT\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        // subscribe
        frame = "SUBSCRIBE\n" + "destination:/topic/" + ((Topic) consumerDestination).getTopicName() + "\n" + "ack:auto\nactivemq.subscriptionName:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("ERROR"));
        assertTrue("contains expected message -" + frame, frame.contains("not allowed"));

        frame = "DISCONNECT\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }


    public void testDurableTopicConsumerStompWithReceipt() throws Exception {

        // verify stomp ok in this case
        StompConnection stompConnection = new StompConnection();
        stompConnection.open("localhost", BrokerRegistry.getInstance().findFirst().getTransportConnectorByScheme("stomp").getPublishableConnectURI().getPort());

        // connect
        String frame = "CONNECT\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("CONNECTED"));

        // subscribe
        frame = "SUBSCRIBE\n" + "destination:/topic/" + ((Topic) consumerDestination).getTopicName() + "\nreceipt:1\n"
                + "ack:auto\nactivemq.subscriptionName:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);

        frame = stompConnection.receiveFrame();
        assertTrue(frame.startsWith("ERROR"));
        assertTrue("contains expected message -" + frame, frame.contains("not allowed"));

        frame = "DISCONNECT\nclient-id:test\n\n" + Stomp.NULL;
        stompConnection.sendFrame(frame);
    }


    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

}
