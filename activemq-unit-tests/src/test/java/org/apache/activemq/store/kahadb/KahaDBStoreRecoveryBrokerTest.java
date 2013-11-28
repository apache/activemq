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
package org.apache.activemq.store.kahadb;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;

import junit.framework.Test;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.RecoveryBrokerTest;
import org.apache.activemq.broker.StubConnection;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.command.SessionInfo;
import org.apache.commons.io.FileUtils;


/**
 * Used to verify that recovery works correctly against
 *
 *
 */
public class KahaDBStoreRecoveryBrokerTest extends RecoveryBrokerTest {
    public static final String KAHADB_DIR_BASE = "target/activemq-data/kahadb";
    public static String kahaDbDirectoryName;

    enum CorruptionType { None, FailToLoad, LoadInvalid, LoadCorrupt, LoadOrderIndex0 };
    public CorruptionType  failTest = CorruptionType.None;

    @Override
    protected void setUp() throws Exception {
        kahaDbDirectoryName = KAHADB_DIR_BASE + "/" + System.currentTimeMillis();
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        try {
            File kahaDbDir = new File(kahaDbDirectoryName);
            FileUtils.deleteDirectory(kahaDbDir);
        } catch (IOException e) {
        }
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File(kahaDbDirectoryName));
        kaha.deleteAllMessages();
        kaha.setCheckForCorruptJournalFiles(failTest == CorruptionType.LoadOrderIndex0);
        broker.setPersistenceAdapter(kaha);
        return broker;
    }

    @Override
    @SuppressWarnings("resource")
    protected BrokerService createRestartedBroker() throws Exception {

        // corrupting index
        File index = new File(kahaDbDirectoryName + "/db.data");
        RandomAccessFile raf = new RandomAccessFile(index, "rw");
        switch (failTest) {
            case FailToLoad:
                index.delete();
                raf = new RandomAccessFile(index, "rw");
                raf.seek(index.length());
                raf.writeBytes("corrupt");
                break;
            case LoadInvalid:
                // page size 0
                raf.seek(0);
                raf.writeBytes("corrupt and cannot load metadata");
                break;
            case LoadCorrupt:
                // loadable but invalid metadata
                // location of order index low priority index for first destination...
                raf.seek(8*1024 + 57);
                raf.writeLong(Integer.MAX_VALUE-10);
                break;
            case LoadOrderIndex0:
                // loadable but invalid metadata
                // location of order index default priority index size
                // so looks like there are no ids in the order index
                // picked up by setCheckForCorruptJournalFiles
                raf.seek(12*1024 + 21);
                raf.writeShort(0);
                raf.writeChar(0);
                raf.writeLong(-1);
                break;
            default:
        }
        raf.close();

        // starting broker
        BrokerService broker = new BrokerService();
        KahaDBStore kaha = new KahaDBStore();
        kaha.setCheckForCorruptJournalFiles(failTest == CorruptionType.LoadOrderIndex0);
        // uncomment if you want to test archiving
        //kaha.setArchiveCorruptedIndex(true);
        kaha.setDirectory(new File(kahaDbDirectoryName));
        broker.setPersistenceAdapter(kaha);
        return broker;
    }

    public static Test suite() {
        return suite(KahaDBStoreRecoveryBrokerTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public void initCombosForTestLargeQueuePersistentMessagesNotLostOnRestart() {
        this.addCombinationValues("failTest", new CorruptionType[]{CorruptionType.FailToLoad, CorruptionType.LoadInvalid, CorruptionType.LoadCorrupt, CorruptionType.LoadOrderIndex0} );
    }

    public void testLargeQueuePersistentMessagesNotLostOnRestart() throws Exception {

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        // Setup the producer and send the message.
        StubConnection connection = createConnection();
        ConnectionInfo connectionInfo = createConnectionInfo();
        SessionInfo sessionInfo = createSessionInfo(connectionInfo);
        ProducerInfo producerInfo = createProducerInfo(sessionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        connection.send(producerInfo);

        ArrayList<String> expected = new ArrayList<String>();

        int MESSAGE_COUNT = 10000;
        for(int i=0; i < MESSAGE_COUNT; i++) {
            Message message = createMessage(producerInfo, destination);
            message.setPersistent(true);
            connection.send(message);
            expected.add(message.getMessageId().toString());
        }
        connection.request(closeConnectionInfo(connectionInfo));

        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        ConsumerInfo consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);
        producerInfo = createProducerInfo(sessionInfo);
        connection.send(producerInfo);

        for(int i=0; i < MESSAGE_COUNT/2; i++) {
            Message m = receiveMessage(connection);
            assertNotNull("Should have received message "+expected.get(0)+" by now!", m);
            assertEquals(expected.remove(0), m.getMessageId().toString());
            MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
            connection.send(ack);
        }

        connection.request(closeConnectionInfo(connectionInfo));

        // restart the broker.
        restartBroker();

        // Setup the consumer and receive the message.
        connection = createConnection();
        connectionInfo = createConnectionInfo();
        sessionInfo = createSessionInfo(connectionInfo);
        connection.send(connectionInfo);
        connection.send(sessionInfo);
        consumerInfo = createConsumerInfo(sessionInfo, destination);
        connection.send(consumerInfo);

        for(int i=0; i < MESSAGE_COUNT/2; i++) {
            Message m = receiveMessage(connection);
            assertNotNull("Should have received message "+expected.get(i)+" by now!", m);
            assertEquals(expected.get(i), m.getMessageId().toString());
            MessageAck ack = createAck(consumerInfo, m, 1, MessageAck.STANDARD_ACK_TYPE);
            connection.send(ack);


        }

        connection.request(closeConnectionInfo(connectionInfo));
    }
}
