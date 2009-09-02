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
package org.apache.activemq.advisory;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import java.util.Vector;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;

/**
 * @version $Revision: 397249 $
 */
public class TempQueueMemoryTest extends EmbeddedBrokerTestSupport {
    protected Connection serverConnection;
    protected Session serverSession;
    protected Connection clientConnection;
    protected Session clientSession;
    protected Destination serverDestination;
    protected int messagesToSend = 2000;
    protected boolean deleteTempQueue = true;
    protected boolean serverTransactional = false;
    protected boolean clientTransactional = false;
    protected int numConsumers = 1;
    protected int numProducers = 1;
    

    public void testConcurrentProducerRequestReply() throws Exception {
        numProducers = 10;
        testLoadRequestReply();
    }
    
    public void testLoadRequestReply() throws Exception {
        for (int i=0; i< numConsumers; i++) {
            serverSession.createConsumer(serverDestination).setMessageListener(new MessageListener() {
                public void onMessage(Message msg) {
                    try {
                        Destination replyTo = msg.getJMSReplyTo();
                        MessageProducer producer = serverSession.createProducer(replyTo);
                        producer.send(replyTo, msg);
                        if (serverTransactional) {
                            serverSession.commit();
                        }
                        producer.close();
                    } catch (Exception e) {
                        // TODO Auto-generated catch block
                        e.printStackTrace();
                    }
                }
            });
        }
        
        class Producer extends Thread {
            private int numToSend;
            public Producer(int numToSend) {
                this.numToSend = numToSend;
            }
            public void run() {     
                try {
                    Session session = clientConnection.createSession(clientTransactional, 
                            clientTransactional ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
                    MessageProducer producer = session.createProducer(serverDestination);
               
                    for (int i =0; i< numToSend; i++) {
                        TemporaryQueue replyTo = session.createTemporaryQueue();
                        MessageConsumer consumer = session.createConsumer(replyTo);
                        Message msg = session.createMessage();
                        msg.setJMSReplyTo(replyTo);
                        producer.send(msg);
                        if (clientTransactional) {
                            session.commit();
                        }
                        consumer.receive();
                        if (clientTransactional) {
                            session.commit();
                        }
                        consumer.close();
                        if (deleteTempQueue) {
                            replyTo.delete();
                        } else {
                            // temp queue will be cleaned up on clientConnection.close
                        }
                    }
                } catch (JMSException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            }
        }
        Vector<Thread> threads = new Vector<Thread>(numProducers);
        for (int i=0; i<numProducers ; i++) {
            threads.add(new Producer(messagesToSend/numProducers));
        }
        startAndJoinThreads(threads);
        
        clientSession.close();
        serverSession.close();
        clientConnection.close();
        serverConnection.close();
        
        AdvisoryBroker ab = (AdvisoryBroker) broker.getBroker().getAdaptor(
                AdvisoryBroker.class);
              
        ///The server destination will be left
        assertTrue(ab.getAdvisoryDestinations().size() == 1);
        
        assertTrue("should be zero but is "+ab.getAdvisoryConsumers().size(),ab.getAdvisoryConsumers().size() == 0);
        assertTrue("should be zero but is "+ab.getAdvisoryProducers().size(),ab.getAdvisoryProducers().size() == 0);
               
        RegionBroker rb = (RegionBroker) broker.getBroker().getAdaptor(
                RegionBroker.class);
        
               
        //serverDestination + 
        assertEquals(6, rb.getDestinationMap().size());          
    }

    private void startAndJoinThreads(Vector<Thread> threads) throws Exception {
        for (Thread thread: threads) {
            thread.start();
        }
        for (Thread thread: threads) {
            thread.join();
        }
    }

    protected void setUp() throws Exception {
        super.setUp();
        serverConnection = createConnection();
        serverConnection.start();
        serverSession = serverConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        clientConnection = createConnection();
        clientConnection.start();
        clientSession = clientConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        serverDestination = createDestination();
    }

    protected void tearDown() throws Exception {
        
        super.tearDown();
        serverTransactional = clientTransactional = false;
        numConsumers = numProducers = 1;
        messagesToSend = 2000;
    }
    
    protected ActiveMQDestination createDestination() {
        return new ActiveMQQueue(getClass().getName());
    }
    
}
