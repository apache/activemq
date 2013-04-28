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
import java.util.Arrays;
import java.util.LinkedList;

/**
 */
public class ReplicatedLevelDBStoreTest extends TestCase {
    protected static final Logger LOG = LoggerFactory.getLogger(ReplicatedLevelDBStoreTest.class);

    public void testReplication() throws Exception {
        LinkedList<File> directories = new LinkedList<File>();
        directories.add(new File("target/activemq-data/leveldb-node1"));
        directories.add(new File("target/activemq-data/leveldb-node2"));
        directories.add(new File("target/activemq-data/leveldb-node3"));

        for( File f: directories) {
            FileSupport.toRichFile(f).recursiveDelete();
        }

        ArrayList<String> expected_list = new ArrayList<String>();
        final int LOG_SIZE = 1023*200;
        // We will rotate between 3 nodes the task of being the master.
        for( int j=0; j < 10; j++) {

            MasterLevelDBStore master = new MasterLevelDBStore();
            master.setDirectory(directories.get(0));
            master.setBind("tcp://0.0.0.0:0");
            master.setSecurityToken("foo");
            master.setMinReplica(1);
            master.setLogSize(LOG_SIZE);
            LOG.info("Starting master: "+master.replicaId());
            master.start();

            SlaveLevelDBStore slave1 = new SlaveLevelDBStore();
            slave1.setDirectory(directories.get(1));
            slave1.setConnect("tcp://127.0.0.1:" + master.getPort());
            slave1.setSecurityToken("foo");
            slave1.setLogSize(LOG_SIZE);
            LOG.info("Starting slave: "+slave1.replicaId());
            slave1.start();

            SlaveLevelDBStore slave2 = new SlaveLevelDBStore();
            slave2.setDirectory(directories.get(2));
            slave2.setConnect("tcp://127.0.0.1:" + master.getPort());
            slave2.setSecurityToken("foo");
            slave2.setLogSize(LOG_SIZE);

            LOG.info("Adding messages...");
            MessageStore ms = master.createQueueMessageStore(new ActiveMQQueue("TEST"));
            final int TOTAL = 500;
            for (int i = 0; i < TOTAL; i++) {
                if (  i % ((int) (TOTAL * 0.10)) == 0) {
                    LOG.info("" + (100*i/TOTAL) + "% done");
                }

                if( i == 100 ) {
                    LOG.info("Starting slave: "+slave2.replicaId());
                    slave2.start();
                }

                if( i == 200 ) {
                    LOG.info("Stopping slave: "+slave2.replicaId());
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
