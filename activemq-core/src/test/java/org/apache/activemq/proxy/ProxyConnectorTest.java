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
package org.apache.activemq.proxy;

import javax.jms.DeliveryMode;

import junit.framework.Test;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;

public class ProxyConnectorTest extends ProxyTestSupport {

    public ActiveMQDestination destination;
    public byte destinationType;
    public int deliveryMode;

    public static Test suite() {
        return suite(ProxyConnectorTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();
    }

    public void initCombosForTestSendAndConsume() {
        addCombinationValues("deliveryMode", new Object[] {Integer.valueOf(DeliveryMode.NON_PERSISTENT), Integer.valueOf(DeliveryMode.PERSISTENT)});
        addCombinationValues("destinationType", new Object[] {Byte.valueOf(ActiveMQDestination.TOPIC_TYPE)});
    }

    public void testSendAndConsume() throws Exception {

        // Start a producer on local broker using the proxy
        StubConnection connection1 = createProxyConnection();
        ConnectionInfo connectionInfo1 = createConnectionInfo();
        SessionInfo sessionInfo1 = createSessionInfo(connectionInfo1);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo1);
        connection1.send(connectionInfo1);
        connection1.send(sessionInfo1);
        connection1.send(producerInfo);

        destination = createDestinationInfo(connection1, connectionInfo1, destinationType);
        ConsumerInfo consumerInfo1 = createConsumerInfo(sessionInfo1, destination);
        connection1.send(consumerInfo1);

        // Start a consumer on a remote broker using a proxy connection.
        StubConnection connection2 = createRemoteProxyConnection();
        ConnectionInfo connectionInfo2 = createConnectionInfo();
        SessionInfo sessionInfo2 = createSessionInfo(connectionInfo2);
        connection2.send(connectionInfo2);
        connection2.send(sessionInfo2);

        ConsumerInfo consumerInfo2 = createConsumerInfo(sessionInfo2, destination);
        connection2.send(consumerInfo2);

        // Give broker enough time to receive and register the consumer info
        // Either that or make consumer retroactive
        try {
            Thread.sleep(2000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Send the message to the local broker.
        connection1.request(createMessage(producerInfo, destination, deliveryMode));

        // Verify that the message Was sent to the remote broker and the local
        // broker.
        Message m;
        m = receiveMessage(connection1);
        assertNotNull(m);
        assertNoMessagesLeft(connection1);

        m = receiveMessage(connection2);
        assertNotNull(m);
        assertNoMessagesLeft(connection2);

    }

}
