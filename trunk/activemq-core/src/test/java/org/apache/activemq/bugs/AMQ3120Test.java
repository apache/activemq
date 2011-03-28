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
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBPersistenceAdapter;
import org.apache.activemq.util.ConsumerThread;
import org.apache.activemq.util.ProducerThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Test;

import javax.jms.*;

import java.io.File;

import static org.junit.Assert.assertEquals;

public class AMQ3120Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ3120Test.class);

    BrokerService broker = null;
    File kahaDbDir = null;
    private final Destination destination = new ActiveMQQueue("AMQ3120Test");
    final String payload = new String(new byte[1024]);

    protected void startBroker(boolean delete) throws Exception {
        broker = new BrokerService();

        //Start with a clean directory
        kahaDbDir = new File(broker.getBrokerDataDirectory(), "KahaDB");
        deleteDir(kahaDbDir);

        broker.setSchedulerSupport(false);
        broker.setDeleteAllMessagesOnStartup(delete);
        broker.setPersistent(true);
        broker.setUseJmx(false);
        broker.addConnector("tcp://localhost:0");

        PolicyMap map = new PolicyMap();
        PolicyEntry entry = new PolicyEntry();
        entry.setUseCache(false);
        map.setDefaultEntry(entry);
        broker.setDestinationPolicy(map);

        configurePersistence(broker, delete);

        broker.start();
        LOG.info("Starting broker..");
    }

    protected void configurePersistence(BrokerService brokerService, boolean deleteAllOnStart) throws Exception {
        KahaDBPersistenceAdapter adapter = (KahaDBPersistenceAdapter) brokerService.getPersistenceAdapter();

        // ensure there are a bunch of data files but multiple entries in each
        adapter.setJournalMaxFileLength(1024 * 20);

        // speed up the test case, checkpoint an cleanup early and often
        adapter.setCheckpointInterval(500);
        adapter.setCleanupInterval(500);

        if (!deleteAllOnStart) {
            adapter.setForceRecoverIndex(true);
        }

    }

    private boolean deleteDir(File dir) {
        if (dir.isDirectory()) {
            String[] children = dir.list();
            for (int i = 0; i < children.length; i++) {
                boolean success = deleteDir(new File(dir, children[i]));
                if (!success) {
                    return false;
                }
            }
        }

        return dir.delete();
    }

    private int getFileCount(File dir){
        if (dir.isDirectory()) {
            String[] children = dir.list();
            return children.length;
        }

        return 0;
    }

    @Test
    public void testCleanupOfFiles() throws Exception {
        final int messageCount = 500;
        startBroker(true);
        int fileCount = getFileCount(kahaDbDir);
        assertEquals(4, fileCount);

        Connection connection = new ActiveMQConnectionFactory(
                broker.getTransportConnectors().get(0).getConnectUri()).createConnection();
        connection.start();
        Session producerSess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Session consumerSess = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ProducerThread producer = new ProducerThread(producerSess, destination) {
            @Override
            protected Message createMessage(int i) throws Exception {
                return sess.createTextMessage(payload + "::" + i);
            }
        };
        producer.setSleep(650);
        producer.setMessageCount(messageCount);
        ConsumerThread consumer = new ConsumerThread(consumerSess, destination);
        consumer.setBreakOnNull(false);
        consumer.setMessageCount(messageCount);

        producer.start();
        consumer.start();

        producer.join();
        consumer.join();

        assertEquals("consumer got all produced messages", producer.getMessageCount(), consumer.getReceived());

        broker.stop();
        broker.waitUntilStopped();

    }

}
