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

import java.io.File;
import java.text.DateFormat;
import java.util.Date;
import java.util.concurrent.CountDownLatch;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.ObjectMessage;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.store.amq.AMQPersistenceAdapter;
import org.apache.activemq.store.amq.AMQPersistenceAdapterFactory;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


public class VerifySteadyEnqueueRate extends TestCase {

    private static final Log LOG = LogFactory.getLog(VerifySteadyEnqueueRate.class);

    private final CountDownLatch latch = new CountDownLatch(max_messages);
    private static int max_messages = 10000000;
    private static int messageCounter;
    private String destinationName = getName()+"_Queue";
    private BrokerService broker;
    private Connection receiverConnection;
    private Connection producerConnection;
    final boolean useTopic = false;
    
    private boolean useAMQPStore=true;
    protected static final String payload = new String(new byte[24]);

    public void setUp() throws Exception {
        messageCounter = 0;
        startBroker();
        receiverConnection = createConnection();
        receiverConnection.start();
        producerConnection = createConnection();
        producerConnection.start();
    }
    
    public void tearDown() throws Exception {
        receiverConnection.close();
        producerConnection.close();
        broker.stop();
    }

    public void testForDataFileNotDeleted() throws Exception {
        if (true) {
            return;
        }
        doTestForDataFileNotDeleted(false);
    }
       
    private void doTestForDataFileNotDeleted(boolean transacted) throws Exception {
        final long min = 100;
        long max = 0;
        long reportTime = 0;
        Receiver receiver = new Receiver() {
            public void receive(String s) throws Exception {
                messageCounter++; 
                latch.countDown();
            }
        };
        //buildReceiver(receiverConnection, destinationName, transacted, receiver, useTopic);

        final MessageSender producer = new MessageSender(destinationName, producerConnection, transacted, useTopic);
        for (int i=0; i< max_messages; i++) {
            long startT = System.currentTimeMillis();
            producer.send(payload );
            long endT = System.currentTimeMillis();
            long duration = endT - startT;
            
            if (duration > max) {
                max = duration;
            }
            
            if (duration > min) {
                System.err.println(DateFormat.getTimeInstance().format(new Date(startT)) + " at message " + i + " send time=" + duration);    
            }
        }
        System.out.println("max = " + max);
        //latch.await();
        //assertEquals(max_messages, messageCounter);
        //waitFordataFilesToBeCleanedUp(persistentAdapter.getAsyncDataManager(), 30000, 2); 
    }

    private Connection createConnection() throws Exception {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getConnectUri());
        return factory.createConnection();
    }

    private void startBroker() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        
        if( useAMQPStore ) {
            AMQPersistenceAdapterFactory factory = (AMQPersistenceAdapterFactory) broker.getPersistenceFactory();
            // ensure there are a bunch of data files but multiple entries in each
            //factory.setMaxFileLength(1024 * 20);
            // speed up the test case, checkpoint an cleanup early and often
            //factory.setCheckpointInterval(500);
            factory.setCleanupInterval(1000*60*30);
            factory.setSyncOnWrite(false);
            
            //int indexBinSize=262144; // good for 6M
            int indexBinSize=1024;
            factory.setIndexMaxBinSize(indexBinSize * 2);
            factory.setIndexBinSize(indexBinSize);
            factory.setIndexPageSize(192*20);
        } else {
            KahaDBStore kaha = new KahaDBStore();
            kaha.setDirectory(new File("target/activemq-data/kahadb"));
            kaha.deleteAllMessages();
            broker.setPersistenceAdapter(kaha);
        }

        broker.addConnector("tcp://localhost:0").setName("Default");
        broker.start();
        LOG.info("Starting broker..");
    }

    private void buildReceiver(Connection connection, final String queueName, boolean transacted, final Receiver receiver, boolean isTopic) throws Exception {
        final Session session = transacted ? connection.createSession(true, Session.SESSION_TRANSACTED) : connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer inputMessageConsumer = session.createConsumer(isTopic ? session.createTopic(queueName) : session.createQueue(queueName));
        MessageListener messageListener = new MessageListener() {

            public void onMessage(Message message) {
                try {
                    ObjectMessage objectMessage = (ObjectMessage)message;
                    String s = (String)objectMessage.getObject();
                    receiver.receive(s);
                    if (session.getTransacted()) {
                        session.commit();
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        inputMessageConsumer.setMessageListener(messageListener);
    }
}
