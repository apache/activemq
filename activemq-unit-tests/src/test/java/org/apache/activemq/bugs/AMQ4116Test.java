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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ActiveMQMessage;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.Assert;

public class AMQ4116Test extends EmbeddedBrokerTestSupport {

    private final String tcpAddr = "tcp://localhost:0";
    private String connectionUri;

    /**
     * In this test, a message is produced and consumed from the test queue.
     * Memory usage on the test queue should be reset to 0. The memory that was
     * consumed is then sent to a second queue. Memory usage on the original
     * test queue should remain 0, but actually increased when the second
     * enqueue occurs.
     */
    public void testVMTransport() throws Exception {
        runTest(connectionFactory);
    }

    /**
     * This is an analog to the previous test, but occurs over TCP and passes.
     */
    public void testTCPTransport() throws Exception {
        runTest(new ActiveMQConnectionFactory(connectionUri));
    }

    private void runTest(ConnectionFactory connFactory) throws Exception {
        // Verify that test queue is empty and not using any memory.
        Destination physicalDestination = broker.getDestination(destination);
        Assert.assertEquals(0, physicalDestination.getMemoryUsage().getUsage());

        // Enqueue a single message and verify that the test queue is using
        // memory.
        Connection conn = connFactory.createConnection();
        conn.start();
        Session session = conn.createSession(true, Session.SESSION_TRANSACTED);
        MessageProducer producer = session.createProducer(destination);

        producer.send(new ActiveMQMessage());

        // Commit, which ensures message is in queue and memory usage updated.
        session.commit();
        Assert.assertTrue(physicalDestination.getMemoryUsage().getUsage() > 0);

        // Consume the message and verify that the test queue is no longer using
        // any memory.
        MessageConsumer consumer = session.createConsumer(destination);
        Message received = consumer.receive();
        Assert.assertNotNull(received);

        // Commit, which ensures message is removed from queue and memory usage
        // updated.
        session.commit();
        Assert.assertEquals(0, physicalDestination.getMemoryUsage().getUsage());

        // Resend the message to a different queue and verify that the original
        // test queue is still not using any memory.
        ActiveMQQueue secondDestination = new ActiveMQQueue(AMQ4116Test.class + ".second");
        MessageProducer secondPproducer = session.createProducer(secondDestination);

        secondPproducer.send(received);

        // Commit, which ensures message is in queue and memory usage updated.
        // NOTE: This assertion fails due to bug.
        session.commit();
        Assert.assertEquals(0, physicalDestination.getMemoryUsage().getUsage());

        conn.stop();
    }

    /**
     * Create an embedded broker that has both TCP and VM connectors.
     */
    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        connectionUri = broker.addConnector(tcpAddr).getPublishableConnectString();
        return broker;
    }
}
