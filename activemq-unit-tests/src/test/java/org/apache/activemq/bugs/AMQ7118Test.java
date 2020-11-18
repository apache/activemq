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
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.commons.lang.StringUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.File;
import java.io.FilenameFilter;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;

public class AMQ7118Test {

    protected static final Logger LOG = LoggerFactory.getLogger(AMQ7118Test.class);

    protected static Random r = new Random();
    final static String WIRE_LEVEL_ENDPOINT = "tcp://localhost:61616";
    protected BrokerService broker;
    protected Connection producerConnection;
    protected Session pSession;
    protected Connection cConnection;
    protected Session cSession;
    private final String xbean = "xbean:";
    private final String confBase = "src/test/resources/org/apache/activemq/bugs/amq7118";
    int checkpointIndex = 0;

    private static final ActiveMQConnectionFactory ACTIVE_MQ_CONNECTION_FACTORY = new ActiveMQConnectionFactory(WIRE_LEVEL_ENDPOINT);

    @Before
    public void setup() throws Exception {
        deleteData(new File("target/data"));
        createBroker();
        ACTIVE_MQ_CONNECTION_FACTORY.setConnectionIDPrefix("bla");
    }

    @After
    public void shutdown() throws Exception {
        broker.stop();
    }

    public void setupProducerConnection() throws Exception {
        producerConnection = ACTIVE_MQ_CONNECTION_FACTORY.createConnection();
        producerConnection.start();
        pSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    public void setupConsumerConnection() throws Exception {
        cConnection = ACTIVE_MQ_CONNECTION_FACTORY.createConnection();
        cConnection.setClientID("myClient1");
        cConnection.start();
        cSession = cConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
    }
    private void createBroker() throws Exception {
        broker = new BrokerService();
        broker = BrokerFactory.createBroker(xbean + confBase + "/activemq.xml");
        broker.start();
    }


    @Test
    public void testCompaction() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);

        setupProducerConnection();
        setupConsumerConnection();

        Topic topic = pSession.createTopic("test");

        MessageConsumer consumer = cSession.createDurableSubscriber(topic, "clientId1");

        LOG.info("Produce message to test topic");
        produce(pSession, topic, 1, 512 ); // just one message

        LOG.info("Consume message from test topic");
        Message msg = consumer.receive(5000);
        assertNotNull(msg);

        LOG.info("Produce more messages to test topic and get into PFC");
        boolean sent = produce(cSession, topic, 20, 512 * 1024); // Fill the store

        assertFalse("Never got to PFC condition", sent);

        LOG.info("PFC hit");

        //We hit PFC, so shut down the producer
        producerConnection.close();

        //Lets check the db-*.log file count before checkpointUpdate
        checkFiles(false, 21, "db-21.log");

        // Force checkFiles update
        checkFiles(true, 23, "db-23.log");

        //The ackMessageFileMap should be clean, so no more writing
        checkFiles(true, 23, "db-23.log");

        //One more time just to be sure - The ackMessageFileMap should be clean, so no more writing
        checkFiles(true, 23, "db-23.log");

        //Read out the rest of the messages
        LOG.info("Consuming the rest of the files...");
        for (int i = 0; i < 20; i++) {
            msg = consumer.receive(5000);
        }
        LOG.info("All messages Consumed.");

        //Clean up the log files and be sure its stable
        checkFiles(true, 2, "db-30.log");
        checkFiles(true, 3, "db-31.log");
        checkFiles(true, 2, "db-31.log");
        checkFiles(true, 2, "db-31.log");
        checkFiles(true, 2, "db-31.log");

        broker.stop();
        broker.waitUntilStopped();
    }

    protected static boolean produce(Session session, Topic topic, int messageCount, int messageSize) throws JMSException {
        MessageProducer producer = session.createProducer(topic);

        for (int i = 0; i < messageCount; i++) {
            TextMessage helloMessage = session.createTextMessage(StringUtils.repeat("a", messageSize));

            try {
                producer.send(helloMessage);
            } catch (ResourceAllocationException e){
                return false;
            }
        }

        return true;
    }

    private void deleteData(File file) {
        String[] entries = file.list();
        if (entries == null) return;
        for (String s : entries) {
            File currentFile = new File(file.getPath(), s);
            if (currentFile.isDirectory()) {
                deleteData(currentFile);
            }
            currentFile.delete();
        }
        file.delete();
    }

    private void checkFiles(boolean doCheckpoint, int expectedCount, String lastFileName) throws Exception {

        File dbfiles = new File("target/data/kahadb");
        FilenameFilter lff = new FilenameFilter(){
            @Override
            public boolean accept(File dir, String name) {
                return name.toLowerCase().startsWith("db-") && name.toLowerCase().endsWith("log");
            }
        };

        if(doCheckpoint) {
            LOG.info("Initiating checkpointUpdate "+ ++checkpointIndex + " ...");
            broker.getPersistenceAdapter().checkpoint(true);
            TimeUnit.SECONDS.sleep(4);
            LOG.info("Checkpoint complete.");
        }
        File files[] = dbfiles.listFiles(lff);
        Arrays.sort(files,  new DBFileComparator() );
        logfiles(files);

        while (files.length != expectedCount) {
            // gives time to checkpoint
            TimeUnit.SECONDS.sleep(1);
        }

        assertEquals(expectedCount, files.length);
        assertEquals(lastFileName, files[files.length-1].getName());

    }

    private void logfiles(File[] files){

        LOG.info("Files found in KahaDB:");
        for (File file : files) {
            LOG.info("    " + file.getName());
        }
    }

    private class DBFileComparator implements Comparator<File> {
        @Override
        public int compare(File o1, File o2) {
            int n1 = extractNumber(o1.getName());
            int n2 = extractNumber(o2.getName());
            return n1 - n2;
        }

        private int extractNumber(String name) {
            int i = 0;
            try {
                int s = name.indexOf('-')+1;
                int e = name.lastIndexOf('.');
                String number = name.substring(s, e);
                i = Integer.parseInt(number);
            } catch(Exception e) {
                i = 0; // if filename does not match the format
                // then default to 0
            }
            return i;
        }
    }
}