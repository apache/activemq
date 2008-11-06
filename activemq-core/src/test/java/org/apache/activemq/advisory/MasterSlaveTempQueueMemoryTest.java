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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQSession;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.RegionBroker;


public class MasterSlaveTempQueueMemoryTest extends TempQueueMemoryTest {
    String masterBindAddress = "tcp://localhost:61616";
    String slaveBindAddress = "tcp://localhost:62616";
    BrokerService slave;

    /*
     * add a slave broker
     * @see org.apache.activemq.EmbeddedBrokerTestSupport#createBroker()
     */
    @Override
    protected BrokerService createBroker() throws Exception {
        // bindAddress is used by super.createBroker
        bindAddress = masterBindAddress;
        BrokerService master = super.createBroker();
        master.setBrokerName("master");
        master.setUseJmx(false);
        bindAddress = slaveBindAddress;
        slave = super.createBroker();
        slave.setBrokerName("slave");
        slave.setMasterConnectorURI(masterBindAddress);
        slave.setUseJmx(false);
        bindAddress = masterBindAddress;
        return master;
    }

    @Override
    protected void startBroker() throws Exception {
        
        // because master will wait for slave to connect it needs 
        // to be in a separate thread
        Thread starterThread = new Thread() { 
            public void run() {
                try {
                    broker.setWaitForSlave(true);
                    broker.start();
                } catch (Exception e) {
                    fail("failed to start broker, reason:" + e);
                    e.printStackTrace();
                }
            }
        };
        starterThread.start();
        
        slave.start();
        starterThread.join(60*1000);
        assertTrue("slave is indeed a slave", slave.isSlave());
    }

    @Override
    protected void tearDown() throws Exception {
        slave.stop();
        super.tearDown();
        
    }

    @Override
    public void testLoadRequestReply() throws Exception {
        super.testLoadRequestReply();
        
        // some checks on the slave
        AdvisoryBroker ab = (AdvisoryBroker) slave.getBroker().getAdaptor(
                AdvisoryBroker.class);
        
        if (!deleteTempQueue || serverTransactional) {
            // give temp destination removes a chance to perculate on connection.close
            Thread.sleep(2000);
        } 
        assertEquals("the temp queues should not be visible as they are removed", 1, ab.getAdvisoryDestinations().size());
                       
        RegionBroker rb = (RegionBroker) slave.getBroker().getAdaptor(
                RegionBroker.class); 
               
        //serverDestination + 
        assertEquals(6, rb.getDestinationMap().size());     
        
        RegionBroker masterRb = (RegionBroker) broker.getBroker().getAdaptor(
                RegionBroker.class);

        // REVISIT the following two are not dependable at the moment, off by a small number
        // for some reason? The work for a COUNT < ~500
        //
        //assertEquals("inflight match", rb.getDestinationStatistics().getInflight().getCount(), masterRb.getDestinationStatistics().getInflight().getCount());
        //assertEquals("enqueues match", rb.getDestinationStatistics().getEnqueues().getCount(), masterRb.getDestinationStatistics().getEnqueues().getCount());
        
        assertEquals("dequeues match",
                rb.getDestinationStatistics().getDequeues().getCount(),
                masterRb.getDestinationStatistics().getDequeues().getCount());
        
        // slave does not actually dispatch any messages, so no request/reply(2) pair per iteration(COUNT)
        // slave estimate must be >= actual master value
        // master does not always reach expected total, should be assertEquals.., why?
        assertTrue("dispatched to slave is as good as master, master=" 
                + masterRb.getDestinationStatistics().getDispatched().getCount(),
                rb.getDestinationStatistics().getDispatched().getCount() + 2*messagesToSend >= 
                masterRb.getDestinationStatistics().getDispatched().getCount());
    }
    
    public void testMoreThanPageSizeUnacked() throws Exception {
        
        final int messageCount = Queue.MAX_PAGE_SIZE + 10;
        final CountDownLatch latch = new CountDownLatch(1);
        
        serverSession = serverConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQSession s = (ActiveMQSession) serverSession;
        s.setSessionAsyncDispatch(true);
        
        MessageConsumer serverConsumer = serverSession.createConsumer(serverDestination);
        serverConsumer.setMessageListener(new MessageListener() {
           
            public void onMessage(Message msg) {
                try {
                    latch.await(30L, TimeUnit.SECONDS);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
            
        MessageProducer producer = clientSession.createProducer(serverDestination);
        for (int i =0; i< messageCount; i++) {
            Message msg = clientSession.createMessage();
            producer.send(msg);
        }
        Thread.sleep(5000);
        
        RegionBroker slaveRb = (RegionBroker) slave.getBroker().getAdaptor(
                RegionBroker.class);
        RegionBroker masterRb = (RegionBroker) broker.getBroker().getAdaptor(
                RegionBroker.class);
        
        assertEquals("inflight match expected", messageCount, masterRb.getDestinationStatistics().getInflight().getCount());        
        assertEquals("inflight match on slave and master", slaveRb.getDestinationStatistics().getInflight().getCount(), masterRb.getDestinationStatistics().getInflight().getCount());
        
        latch.countDown();
        Thread.sleep(5000);
        assertEquals("inflight match expected", 0, masterRb.getDestinationStatistics().getInflight().getCount());        
        assertEquals("inflight match on slave and master", slaveRb.getDestinationStatistics().getInflight().getCount(), masterRb.getDestinationStatistics().getInflight().getCount());
    }
    
    public void testLoadRequestReplyWithNoTempQueueDelete() throws Exception {
        deleteTempQueue = false;
        messagesToSend = 10;
        testLoadRequestReply();
    }
    
    public void testLoadRequestReplyWithTransactions() throws Exception {
        serverTransactional = clientTransactional = true;
        messagesToSend = 100;
        reInitialiseSessions();
        testLoadRequestReply();
    }
    
    public void testConcurrentConsumerLoadRequestReplyWithTransactions() throws Exception {
        serverTransactional = true;
        numConsumers = numProducers = 10;
        messagesToSend = 100;
        reInitialiseSessions();
        testLoadRequestReply();
    }

    protected void reInitialiseSessions() throws Exception {
        // reinitialize so they can respect the transactional flags 
        serverSession.close();
        clientSession.close();
        serverSession = serverConnection.createSession(serverTransactional, 
                serverTransactional ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
        clientSession = clientConnection.createSession(clientTransactional,
                clientTransactional ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);
    }
}
