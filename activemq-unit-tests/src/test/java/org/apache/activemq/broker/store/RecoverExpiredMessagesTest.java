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
package org.apache.activemq.broker.store;

import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import junit.framework.Test;
import org.apache.activemq.broker.BrokerRestartTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.broker.region.policy.FilePendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.store.jdbc.JDBCPersistenceAdapter;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;

public class RecoverExpiredMessagesTest extends BrokerRestartTestSupport {
    final ArrayList<String> expected = new ArrayList<String>();
    final ActiveMQDestination destination = new ActiveMQQueue("TEST");
    public PendingQueueMessageStoragePolicy queuePendingPolicy;
    public PersistenceAdapter persistenceAdapter;

    @Override
    protected void setUp() throws Exception {
        setAutoFail(true);
        super.setUp();
    }

    public void initCombosForTestRecovery() throws Exception {
        addCombinationValues("queuePendingPolicy", new PendingQueueMessageStoragePolicy[] {new FilePendingQueueMessageStoragePolicy(), new VMPendingQueueMessageStoragePolicy()});
        addCombinationValues("persistenceAdapter", new PersistenceAdapter[] {new KahaDBPersistenceAdapter(), new JDBCPersistenceAdapter()});
    }

    public void testRecovery() throws Exception {
        sendSomeMessagesThatWillExpireIn5AndThenOne();

        broker.stop();
        TimeUnit.SECONDS.sleep(6);
        broker = createRestartedBroker();
        broker.start();

        consumeExpected();
    }

    private void consumeExpected() throws Exception {
        // Setup the consumer and receive the message.
         StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        Message m = receiveMessage(connection);
        assertNotNull("Should have received message " + expected.get(0) + " by now!", m);
        assertEquals(expected.get(0), m.getMessageId().toString());
        MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
        connection.send(ack);

        assertNoMessagesLeft(connection);
        connection.request(closeConnectionInfo(connectionInfo));
    }

    private void sendSomeMessagesThatWillExpireIn5AndThenOne() throws Exception {

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);


        int MESSAGE_COUNT = 10;
        for(int i=0; i < MESSAGE_COUNT; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setExpiration(System.currentTimeMillis()+5000);
            message.setPersistent(true);
            connection.send(message);
        }
        Message message = createMessage(producerInfo, destination);
        message.setPersistent(true);
        connection.send(message);
        expected.add(message.getMessageId().toString());

        connection.request(closeConnectionInfo(connectionInfo));
    }

    @Override
    protected PolicyEntry getDefaultPolicy() {
        PolicyEntry policy = super.getDefaultPolicy();
        policy.setPendingQueuePolicy(queuePendingPolicy);
        policy.setExpireMessagesPeriod(0);
        return policy;
    }

    @Override
    protected void configureBroker(BrokerService broker) throws Exception {
        super.configureBroker(broker);
        broker.setPersistenceAdapter(persistenceAdapter);
    }

    public static Test suite() {
        return suite(RecoverExpiredMessagesTest.class);
    }

}
