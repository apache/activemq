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
package org.apache.activemq.leveldb.test;

import junit.framework.TestCase;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.leveldb.CountDownFuture;
import org.apache.activemq.leveldb.LevelDBStore;
import org.apache.activemq.leveldb.replicated.MasterLevelDBStore;
import org.apache.activemq.leveldb.replicated.SlaveLevelDBStore;
import org.apache.activemq.leveldb.util.FileSupport;
import org.apache.activemq.store.MessageRecoveryListener;
import org.apache.activemq.store.MessageStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.JMSException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 */
public class ReplicatedLevelDBStoreTest extends TestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(ReplicatedLevelDBStoreTest.class);

    public void testMinReplicaEnforced() throws Exception {

        File masterDir = new File("target/activemq-data/leveldb-node1");
        File slaveDir = new File("target/activemq-data/leveldb-node2");
        FileSupport.toRichFile(masterDir).recursiveDelete();
        FileSupport.toRichFile(slaveDir).recursiveDelete();

        MasterLevelDBStore master = createMaster(masterDir);
        master.setMinReplica(1);
        master.start();

        MessageStore ms = master.createQueueMessageStore(new ActiveMQQueue("TEST"));

        // Updating the store should not complete since we don't have enough
        // replicas.
        CountDownFuture f = asyncAddMessage(ms, "m1");
        assertFalse(f.completed().await(2, TimeUnit.SECONDS));

        // Adding a slave should allow that update to complete.
        SlaveLevelDBStore slave = createSlave(master, slaveDir);
        slave.start();

        assertTrue(f.completed().await(2, TimeUnit.SECONDS));

        // New updates should complete quickly now..
        f = asyncAddMessage(ms, "m2");
        assertTrue(f.completed().await(1, TimeUnit.SECONDS));

        // If the slave goes offline, then updates should once again
        // not complete.
        slave.stop();

        f = asyncAddMessage(ms, "m3");
        assertFalse(f.completed().await(2, TimeUnit.SECONDS));

        // Restart and the op should complete.
        slave = createSlave(master, slaveDir);
        slave.start();
        assertTrue(f.completed().await(2, TimeUnit.SECONDS));

        master.stop();
        slave.stop();

    }

    private CountDownFuture asyncAddMessage(final MessageStore ms, final String body) {
        final CountDownFuture f = new CountDownFuture(new CountDownLatch(1));
        LevelDBStore.BLOCKING_EXECUTOR().execute(new Runnable() {
            public void run() {
                try {
                    addMessage(ms, body);
                } catch (Exception e) {
                    e.printStackTrace();
                } finally {
                    f.countDown();
                }
            }
        });
        return f;
    }


    public void testReplication() throws Exception {

        LinkedList<File> directories = new LinkedList<File>();
        directories.add(new File("target/activemq-data/leveldb-node1"));
        directories.add(new File("target/activemq-data/leveldb-node2"));
        directories.add(new File("target/activemq-data/leveldb-node3"));

        for( File f: directories) {
            FileSupport.toRichFile(f).recursiveDelete();
        }

        ArrayList<String> expected_list = new ArrayList<String>();
        // We will rotate between 3 nodes the task of being the master.
        for( int j=0; j < 10; j++) {

            MasterLevelDBStore master = createMaster(directories.get(0));
            master.start();
            SlaveLevelDBStore slave1 = createSlave(master, directories.get(1));
            SlaveLevelDBStore slave2 = createSlave(master, directories.get(2));
            slave2.start();

            LOG.info("Adding messages...");
            MessageStore ms = master.createQueueMessageStore(new ActiveMQQueue("TEST"));
            final int TOTAL = 500;
            for (int i = 0; i < TOTAL; i++) {
                if (  i % ((int) (TOTAL * 0.10)) == 0) {
                    LOG.info("" + (100*i/TOTAL) + "% done");
                }

                if( i == 250 ) {
                    slave1.start();
                    slave2.stop();
                }

                String msgid = "m:" + j + ":" + i;
                addMessage(ms, msgid);
                expected_list.add(msgid);
            }

            LOG.info("Checking master state");
            assertEquals(expected_list, getMessages(ms));

            LOG.info("Stopping master: "+master.replicaId());
            master.stop();
            LOG.info("Stopping slave: "+slave1.replicaId());
            slave1.stop();

            // Rotate the dir order so that slave1 becomes the master next.
            directories.addLast(directories.removeFirst());
        }
    }

    private SlaveLevelDBStore createSlave(MasterLevelDBStore master, File directory) {
        SlaveLevelDBStore slave1 = new SlaveLevelDBStore();
        slave1.setDirectory(directory);
        slave1.setConnect("tcp://127.0.0.1:" + master.getPort());
        slave1.setSecurityToken("foo");
        slave1.setLogSize(1023*200);
        return slave1;
    }

    private MasterLevelDBStore createMaster(File directory) {
        MasterLevelDBStore master = new MasterLevelDBStore();
        master.setDirectory(directory);
        master.setBind("tcp://0.0.0.0:0");
        master.setSecurityToken("foo");
        master.setMinReplica(1);
        master.setLogSize(1023 * 200);
        return master;
    }

    long id_counter = 0L;
    String payload = "";
    {
        for (int i = 0; i < 1024; i++) {
            payload += "x";
        }
    }

    public ActiveMQTextMessage addMessage(MessageStore ms, String body) throws JMSException, IOException {
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setPersistent(true);
        message.setResponseRequired(true);
        message.setStringProperty("id", body);
        message.setText(payload);
        id_counter += 1;
        MessageId messageId = new MessageId("ID:localhost-56913-1254499826208-0:0:1:1:" + id_counter);
        messageId.setBrokerSequenceId(id_counter);
        message.setMessageId(messageId);
        ms.addMessage(new ConnectionContext(), message);
        return message;
    }

    public ArrayList<String> getMessages(MessageStore ms) throws Exception {
        final ArrayList<String> rc = new ArrayList<String>();
        ms.recover(new MessageRecoveryListener() {
            public boolean recoverMessage(Message message) throws Exception {
                rc.add(((ActiveMQTextMessage) message).getStringProperty("id"));
                return true;
            }

            public boolean hasSpace() {
                return true;
            }

            public boolean recoverMessageReference(MessageId ref) throws Exception {
                return true;
            }

            public boolean isDuplicate(MessageId ref) {
                return false;
            }
        });
        return rc;
    }

}
