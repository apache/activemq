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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import javax.jms.DeliveryMode;
import javax.jms.MessageNotWriteableException;

import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.broker.region.policy.FixedCountSubscriptionRecoveryPolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.RoundRobinDispatchPolicy;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageDispatch;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.RemoveInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.store.PersistenceAdapter;
import org.apache.activemq.usage.SystemUsage;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class BrokerTestSupport extends CombinationTestSupport {

    /**
     * Setting this to false makes the test run faster but they may be less
     * accurate.
     */
    public static final boolean FAST_NO_MESSAGE_LEFT_ASSERT = System.getProperty("FAST_NO_MESSAGE_LEFT_ASSERT", "true").equals("true");

    protected RegionBroker regionBroker;
    protected BrokerService broker;
    protected long idGenerator;
    protected int msgIdGenerator;
    protected int txGenerator;
    protected int tempDestGenerator;
    protected PersistenceAdapter persistenceAdapter;

    protected int maxWait = 4000;

    protected SystemUsage memoryManager;

    protected void setUp() throws Exception {
        super.setUp();
        broker = createBroker();
        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(getDefaultPolicy());
        broker.setDestinationPolicy(policyMap);
        broker.start();
    }

    protected PolicyEntry getDefaultPolicy() {
        PolicyEntry policy = new PolicyEntry();
        policy.setDispatchPolicy(new RoundRobinDispatchPolicy());
        policy.setSubscriptionRecoveryPolicy(new FixedCountSubscriptionRecoveryPolicy());
        return policy;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:()/localhost?persistent=false"));
        return broker;
    }

    protected void tearDown() throws Exception {
        broker.stop();
        broker = null;
        regionBroker = null;
        persistenceAdapter = null;
        memoryManager = null;
        super.tearDown();
    }

    protected ConsumerInfo createConsumerInfo(SessionInfo sessionInfo, ActiveMQDestination destination) throws Exception {
        ConsumerInfo info = new ConsumerInfo(sessionInfo, ++idGenerator);
        info.setBrowser(false);
        info.setDestination(destination);
        info.setPrefetchSize(1000);
        info.setDispatchAsync(false);
        return info;
    }

    protected RemoveInfo closeConsumerInfo(ConsumerInfo consumerInfo) {
        return consumerInfo.createRemoveCommand();
    }

    protected ProducerInfo createProducerInfo(SessionInfo sessionInfo) throws Exception {
        ProducerInfo info = new ProducerInfo(sessionInfo, ++idGenerator);
        return info;
    }

    protected SessionInfo createSessionInfo(ConnectionInfo connectionInfo) throws Exception {
        SessionInfo info = new SessionInfo(connectionInfo, ++idGenerator);
        return info;
    }

    protected ConnectionInfo createConnectionInfo() throws Exception {
        ConnectionInfo info = new ConnectionInfo();
        info.setConnectionId(new ConnectionId("connection:" + (++idGenerator)));
        info.setClientId(info.getConnectionId().getValue());
        return info;
    }

    protected Message createMessage(ProducerInfo producerInfo, ActiveMQDestination destination) {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setMessageId(new MessageId(producerInfo, ++msgIdGenerator));
        message.setDestination(destination);
        message.setPersistent(false);
        try {
            message.setText("Test Message Payload.");
        } catch (MessageNotWriteableException e) {
        }
        return message;
    }

    protected MessageAck createAck(ConsumerInfo consumerInfo, Message msg, int count, byte ackType) {
        MessageAck ack = new MessageAck();
        ack.setAckType(ackType);
        ack.setConsumerId(consumerInfo.getConsumerId());
        ack.setDestination(msg.getDestination());
        ack.setLastMessageId(msg.getMessageId());
        ack.setMessageCount(count);
        return ack;
    }

    protected void gc() {
        regionBroker.gc();
    }

    protected void profilerPause(String prompt) throws IOException {
        if (System.getProperty("profiler") != null) {
            System.out.println();
            System.out.println(prompt + "> Press enter to continue: ");
            while (System.in.read() != '\n') {
            }
            System.out.println(prompt + "> Done.");
        }
    }

    protected RemoveInfo closeConnectionInfo(ConnectionInfo info) {
        return info.createRemoveCommand();
    }

    protected RemoveInfo closeSessionInfo(SessionInfo info) {
        return info.createRemoveCommand();
    }

    protected RemoveInfo closeProducerInfo(ProducerInfo info) {
        return info.createRemoveCommand();
    }

    protected Message createMessage(ProducerInfo producerInfo, ActiveMQDestination destination, int deliveryMode) {
        Message message = createMessage(producerInfo, destination);
        message.setPersistent(deliveryMode == DeliveryMode.PERSISTENT);
        return message;
    }

    protected LocalTransactionId createLocalTransaction(SessionInfo info) {
        LocalTransactionId id = new LocalTransactionId(info.getSessionId().getParentId(), ++txGenerator);
        return id;
    }

    protected XATransactionId createXATransaction(SessionInfo info) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream os = new DataOutputStream(baos);
        os.writeLong(++txGenerator);
        os.close();
        byte[] bs = baos.toByteArray();

        XATransactionId xid = new XATransactionId();
        xid.setBranchQualifier(bs);
        xid.setGlobalTransactionId(bs);
        xid.setFormatId(55);
        return xid;
    }

    protected TransactionInfo createBeginTransaction(ConnectionInfo connectionInfo, TransactionId txid) {
        TransactionInfo info = new TransactionInfo(connectionInfo.getConnectionId(), txid, TransactionInfo.BEGIN);
        return info;
    }

    protected TransactionInfo createPrepareTransaction(ConnectionInfo connectionInfo, TransactionId txid) {
        TransactionInfo info = new TransactionInfo(connectionInfo.getConnectionId(), txid, TransactionInfo.PREPARE);
        return info;
    }

    protected TransactionInfo createCommitTransaction1Phase(ConnectionInfo connectionInfo, TransactionId txid) {
        TransactionInfo info = new TransactionInfo(connectionInfo.getConnectionId(), txid, TransactionInfo.COMMIT_ONE_PHASE);
        return info;
    }

    protected TransactionInfo createCommitTransaction2Phase(ConnectionInfo connectionInfo, TransactionId txid) {
        TransactionInfo info = new TransactionInfo(connectionInfo.getConnectionId(), txid, TransactionInfo.COMMIT_TWO_PHASE);
        return info;
    }

    protected TransactionInfo createRollbackTransaction(ConnectionInfo connectionInfo, TransactionId txid) {
        TransactionInfo info = new TransactionInfo(connectionInfo.getConnectionId(), txid, TransactionInfo.ROLLBACK);
        return info;
    }

    protected int countMessagesInQueue(StubConnection connection, ConnectionInfo connectionInfo, ActiveMQDestination destination) throws Exception {

        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        consumerInfo.setPrefetchSize(1);
        consumerInfo.setBrowser(true);
        connection.send(consumerInfo);

        ArrayList<Object> skipped = new ArrayList<Object>();

        // Now get the messages.
        Object m = connection.getDispatchQueue().poll(maxWait, TimeUnit.MILLISECONDS);
        int i = 0;
        while (m != null) {
            if (m instanceof MessageDispatch && ((MessageDispatch)m).getConsumerId().equals(consumerInfo.getConsumerId())) {
                MessageDispatch md = (MessageDispatch)m;
                if (md.getMessage() != null) {
                    i++;
                    connection.send(createAck(consumerInfo, md.getMessage(), 1, MessageAck.STANDARD_ACK_TYPE));
                } else {
                    break;
                }
            } else {
                skipped.add(m);
            }
            m = connection.getDispatchQueue().poll(maxWait, TimeUnit.MILLISECONDS);
        }

        for (Iterator<Object> iter = skipped.iterator(); iter.hasNext();) {
            connection.getDispatchQueue().put(iter.next());
        }

        connection.send(closeSessionInfo(sessionInfo));
        return i;

    }

    protected DestinationInfo createTempDestinationInfo(ConnectionInfo connectionInfo, byte destinationType) {
        DestinationInfo info = new DestinationInfo();
        info.setConnectionId(connectionInfo.getConnectionId());
        info.setOperationType(DestinationInfo.ADD_OPERATION_TYPE);
        info.setDestination(ActiveMQDestination.createDestination(info.getConnectionId() + ":" + (++tempDestGenerator), destinationType));
        return info;
    }

    protected ActiveMQDestination createDestinationInfo(StubConnection connection, ConnectionInfo connectionInfo1, byte destinationType) throws Exception {
        if ((destinationType & ActiveMQDestination.TEMP_MASK) != 0) {
            DestinationInfo info = createTempDestinationInfo(connectionInfo1, destinationType);
            connection.send(info);
            return info.getDestination();
        } else {
            return ActiveMQDestination.createDestination("TEST", destinationType);
        }
    }

    protected DestinationInfo closeDestinationInfo(DestinationInfo info) {
        info.setOperationType(DestinationInfo.REMOVE_OPERATION_TYPE);
        info.setTimeout(0);
        return info;
    }

    public static void recursiveDelete(File f) {
        if (f.isDirectory()) {
            File[] files = f.listFiles();
            for (int i = 0; i < files.length; i++) {
                recursiveDelete(files[i]);
            }
        }
        f.delete();
    }

    protected StubConnection createConnection() throws Exception {
        return new StubConnection(broker);
    }

    /**
     * @param connection
     * @return
     * @throws InterruptedException
     */
    public Message receiveMessage(StubConnection connection) throws InterruptedException {
        return receiveMessage(connection, maxWait);
    }

    public Message receiveMessage(StubConnection connection, long timeout) throws InterruptedException {
        while (true) {
            Object o = connection.getDispatchQueue().poll(timeout, TimeUnit.MILLISECONDS);

            if (o == null) {
                return null;
            }
            if (o instanceof MessageDispatch) {

                MessageDispatch dispatch = (MessageDispatch)o;
                if (dispatch.getMessage() == null) {
                    return null;
                }
                dispatch.setMessage(dispatch.getMessage().copy());
                dispatch.getMessage().setRedeliveryCounter(dispatch.getRedeliveryCounter());
                return dispatch.getMessage();
            }
        }
    };

    protected void assertNoMessagesLeft(StubConnection connection) throws InterruptedException {
        long wait = FAST_NO_MESSAGE_LEFT_ASSERT ? 0 : maxWait;
        while (true) {
            Object o = connection.getDispatchQueue().poll(wait, TimeUnit.MILLISECONDS);
            if (o == null) {
                return;
            }
            if (o instanceof MessageDispatch && ((MessageDispatch)o).getMessage() != null) {
                fail("Received a message.");
            }
        }
    }

}
