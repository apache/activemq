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

package org.apache.activemq.usecases;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.FilePendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.junit.Before;
import org.junit.Test;

public class UnlimitedEnqueueTest  {

    BrokerService brokerService = null;
    final long numMessages = 50000;
    final long numThreads = 10;

    @Test
    public void testEnqueueIsOnlyLimitedByDisk() throws Exception {
        ExecutorService executor = Executors.newCachedThreadPool();
        for (int i=0; i<numThreads; i++) {
            executor.execute(new Producer(numMessages/numThreads));
        }
        
        executor.shutdown();
        executor.awaitTermination(30*60, TimeUnit.SECONDS);
    }
    
    @Before
    public void createBrokerService() throws Exception {
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setAdvisorySupport(false);
        
        // optional, reduce the usage limit so that spooling will occur faster
        brokerService.getSystemUsage().getMemoryUsage().setLimit(10 * 1024 * 1024);
        PolicyMap policyMap = new PolicyMap();
        List<PolicyEntry> entries = new ArrayList<PolicyEntry>();
        PolicyEntry policy = new PolicyEntry();
        
        // NB: ensure queue cursor limit is below the default 70% usage that the destination will use
        // if they are the same, the queue memory limit and flow control will kick in first
        policy.setCursorMemoryHighWaterMark(20);
        
        // on by default
        //policy.setProducerFlowControl(true);
        policy.setQueue(">");
        
        // policy that will spool references to disk
        policy.setPendingQueuePolicy(new FilePendingQueueMessageStoragePolicy());
        entries.add(policy);
        policyMap.setPolicyEntries(entries);
        brokerService.setDestinationPolicy(policyMap);
        
        brokerService.start();
    }
    
    public class Producer implements Runnable{

        private final long numberOfMessages;

        public Producer(final long n){
            this.numberOfMessages = n;
        }

        public void run(){
            ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(brokerService.getVmConnectorURI());
            try {
                Connection conn = factory.createConnection();
                conn.start();
                for (int i = 0; i < numberOfMessages; i++) {
                    Session session = conn.createSession(false,Session.AUTO_ACKNOWLEDGE);
                    Destination destination = session.createQueue("test-queue");
                    MessageProducer producer = session.createProducer(destination);
                    producer.setDeliveryMode(DeliveryMode.PERSISTENT);
                    BytesMessage message = session.createBytesMessage();
                    byte[] bytes = new byte[1024*10];
                    message.writeBytes(bytes);
                    try {
                        producer.send(message);
                    } catch (ResourceAllocationException e) {
                        e.printStackTrace();
                    }
                    session.close();
                }
            } catch (JMSException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
