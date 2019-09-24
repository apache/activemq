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

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQXAConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.jms.*;
import javax.transaction.xa.XAResource;
import javax.transaction.xa.Xid;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class AMQ7185Test
{
    private final String xaDestinationName = "DestinationXA";
    private BrokerService broker;
    private String connectionUri;
    private long txGenerator = System.currentTimeMillis();

    private XAConnectionFactory xaConnectionFactory;
    private ConnectionFactory connectionFactory;

    final Topic dest = new ActiveMQTopic(xaDestinationName);

    @Before
    public void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(false);
        broker.setAdvisorySupport(false);
        broker.addConnector("tcp://0.0.0.0:0?trace=true");
        broker.start();
        broker.waitUntilStarted();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();

        connectionFactory = new ActiveMQConnectionFactory(connectionUri);
        ((ActiveMQConnectionFactory) connectionFactory).setWatchTopicAdvisories(false);
        // failover ensure audit is in play
        xaConnectionFactory = new ActiveMQXAConnectionFactory("failover://" + connectionUri);
        ((ActiveMQXAConnectionFactory) xaConnectionFactory).setWatchTopicAdvisories(false);
    }

    @After
    public void stopBroker() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test
    public void testRollbackRedeliveryNoDup() throws Exception {

        XAConnection xaConnection = xaConnectionFactory.createXAConnection();
        xaConnection.setClientID("cid0");
        xaConnection.start();
        XASession session = xaConnection.createXASession();
        TopicSubscriber consumer = session.createDurableSubscriber(dest, "sub");
        consumer.close();
        session.close();
        xaConnection.close();

        publish(dest);

        Xid tid;
        TextMessage receivedMessage;
        xaConnection = xaConnectionFactory.createXAConnection();
        xaConnection.setClientID("cid0");
        xaConnection.start();
        session = xaConnection.createXASession();
        consumer = session.createDurableSubscriber(dest, "sub");

        tid = createXid();
        XAResource resource = session.getXAResource();
        resource.start(tid, XAResource.TMNOFLAGS);

        receivedMessage = (TextMessage) consumer.receive(4000);
        assertNotNull(receivedMessage);
        resource.end(tid, XAResource.TMSUCCESS);
        resource.rollback(tid);
        consumer.close();
        session.close();
        xaConnection.close();


        // redelivery
        xaConnection = xaConnectionFactory.createXAConnection();
        xaConnection.setClientID("cid0");
        xaConnection.start();
        session = xaConnection.createXASession();
        consumer = session.createDurableSubscriber(dest, "sub");

        tid = createXid();
        resource = session.getXAResource();
        resource.start(tid, XAResource.TMNOFLAGS);
        receivedMessage = (TextMessage) consumer.receive(1000);
        assertNotNull(receivedMessage);

        // verify only one
        receivedMessage = (TextMessage) consumer.receiveNoWait();
        assertNull(receivedMessage);

        resource.end(tid, XAResource.TMSUCCESS);
        resource.commit(tid, true);

        consumer.close();
        session.close();
        xaConnection.close();

        // assertNoMessageInDLQ
        assertEquals("Only one enqueue", 1, broker.getAdminView().getTotalEnqueueCount());
    }

    private void publish(Topic dest) throws JMSException {
        Connection connection = connectionFactory.createConnection();
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session.createProducer(dest).send(new ActiveMQTextMessage());
        connection.close();
    }


    public Xid createXid() throws IOException {

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(++txGenerator);
        os.close();
        final byte[] bs = baos.toByteArray();

        return new Xid() {
            public int getFormatId() {
                return 86;
            }

            public byte[] getGlobalTransactionId() {
                return bs;
            }

            public byte[] getBranchQualifier() {
                return bs;
            }
        };
    }
}
