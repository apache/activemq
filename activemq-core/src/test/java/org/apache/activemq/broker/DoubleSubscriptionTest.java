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

import javax.jms.DeliveryMode;

import junit.framework.Test;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.network.NetworkTestSupport;

/**
 * Pretend to be an abusive client that sends multiple identical ConsumerInfo
 * commands and make sure the broker doesn't stall because of it.
 */

public class DoubleSubscriptionTest extends NetworkTestSupport {

    public ActiveMQDestination destination;
    public int deliveryMode;

    private String remoteURI = "tcp://localhost:0?wireFormat.tcpNoDelayEnabled=true";

    public static Test suite() {
        return suite(DoubleSubscriptionTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombosForTestDoubleSubscription() {
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQQueue("TEST"),});
    }

    public void testDoubleSubscription() throws Exception {

        // Start a normal consumer on the remote broker
        StubConnection connection1 = createRemoteConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.request(consumerInfo1);

        // Start a normal producer on a remote broker
        StubConnection connection2 = createRemoteConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        ProducerInfo producerInfo2 = createProducerInfo(sessionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);
        connection2.request(producerInfo2);

        // Send a message to make sure the basics are working
        connection2.request(createMessage(producerInfo2, destination, DeliveryMode.PERSISTENT));

        Message m1 = receiveMessage(connection1);
        assertNotNull(m1);
        assertNoMessagesLeft(connection1);

        connection1.send(createAck(consumerInfo1, m1, 1, MessageAck.STANDARD_ACK_TYPE));

        // Send a message to sit on the broker while we mess with it
        connection2.request(createMessage(producerInfo2, destination, DeliveryMode.PERSISTENT));

        // Now we're going to resend the same consumer commands again and see if
        // the broker
        // can handle it.
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.request(consumerInfo1);

        // After this there should be 2 messages on the broker...
        connection2.request(createMessage(producerInfo2, destination, DeliveryMode.PERSISTENT));

        // ... let's start a fresh consumer...
        connection1.stop();
        StubConnection connection3 = createRemoteConnection();
        ConnectionInfo connectionInfo3 = createConnectionInfo();
        SessionInfo sessionInfo3 = createSessionInfo(connectionInfo3);
        ConsumerInfo consumerInfo3 = createConsumerInfo(sessionInfo3, destination);
        connection3.send(connectionInfo3);
        connection3.send(sessionInfo3);
        connection3.request(consumerInfo3);

        // ... and then grab the 2 that should be there.
        assertNotNull(receiveMessage(connection3));
        assertNotNull(receiveMessage(connection3));
        assertNoMessagesLeft(connection3);
    }

    protected String getRemoteURI() {
        return remoteURI;
    }

}
