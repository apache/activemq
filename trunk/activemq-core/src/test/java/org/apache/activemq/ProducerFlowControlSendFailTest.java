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
package org.apache.activemq;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.ConnectionFactory;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ResourceAllocationException;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.VMPendingSubscriberMessageStoragePolicy;

public class ProducerFlowControlSendFailTest extends ProducerFlowControlTest {

    protected BrokerService createBroker() throws Exception {
        BrokerService service = new BrokerService();
        service.setPersistent(false);
        service.setUseJmx(false);

        // Setup a destination policy where it takes only 1 message at a time.
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setMemoryLimit(1);
        policy.setPendingSubscriberPolicy(new VMPendingSubscriberMessageStoragePolicy());
        policy.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        policy.setProducerFlowControl(true);
        policyMap.setDefaultEntry(policy);
        service.setDestinationPolicy(policyMap);
        
        service.getSystemUsage().setSendFailIfNoSpace(true);

        connector = service.addConnector("tcp://localhost:0");
        return service;
    }
    
    @Override
    public void test2ndPubisherWithStandardConnectionThatIsBlocked() throws Exception {
        // with sendFailIfNoSpace set, there is no blocking of the connection
    }
    
    @Override
    public void testAsyncPubisherRecoverAfterBlock() throws Exception {
        // sendFail means no flowControllwindow as there is no producer ack, just an exception
    }
    
    @Override
    public void testPubisherRecoverAfterBlock() throws Exception {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory)createConnectionFactory();
        // with sendFail, there must be no flowControllwindow
        // sendFail is an alternative flow control mechanism that does not block
        factory.setUseAsyncSend(true);
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();

        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageProducer producer = session.createProducer(queueA);
        
        final AtomicBoolean keepGoing = new AtomicBoolean(true);
   
        Thread thread = new Thread("Filler") {
            @Override
            public void run() {
                while (keepGoing.get()) {
                    try {
                        producer.send(session.createTextMessage("Test message"));
                        if (gotResourceException.get()) {
                            // do not flood the broker with requests when full as we are sending async and they 
                            // will be limited by the network buffers
                            Thread.sleep(200);
                        }
                    } catch (Exception e) {
                        // with async send, there will be no exceptions
                        e.printStackTrace();
                    }
                }
            }
        };
        thread.start();
        waitForBlockedOrResourceLimit(new AtomicBoolean(false));

        // resourceException on second message, resumption if we
        // can receive 10
        MessageConsumer consumer = session.createConsumer(queueA);
        TextMessage msg;
        for (int idx = 0; idx < 10; ++idx) {
            msg = (TextMessage) consumer.receive(1000);
            if (msg != null) {
                msg.acknowledge();
            }
        }
        keepGoing.set(false);
    }

    public void testPubisherRecoverAfterBlockWithSyncSend() throws Exception {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory)createConnectionFactory();
        factory.setExceptionListener(null);
        factory.setUseAsyncSend(false);
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();

        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageProducer producer = session.createProducer(queueA);
        
        final AtomicBoolean keepGoing = new AtomicBoolean(true);
        final AtomicInteger exceptionCount = new AtomicInteger(0);
        Thread thread = new Thread("Filler") {
            @Override
            public void run() {
                while (keepGoing.get()) {
                    try {
                        producer.send(session.createTextMessage("Test message"));
                    } catch (JMSException arg0) {
                        if (arg0 instanceof ResourceAllocationException) {
                            gotResourceException.set(true);
                            exceptionCount.incrementAndGet();
                        }
                    }
                }
            }
        };
        thread.start();
        waitForBlockedOrResourceLimit(new AtomicBoolean(false));

        // resourceException on second message, resumption if we
        // can receive 10
        MessageConsumer consumer = session.createConsumer(queueA);
        TextMessage msg;
        for (int idx = 0; idx < 10; ++idx) {
            msg = (TextMessage) consumer.receive(1000);
            if (msg != null) {
                msg.acknowledge();
            }
        }
        assertTrue("we were blocked at least 5 times", 5 < exceptionCount.get());
        keepGoing.set(false);
    }
    
	@Override
	protected ConnectionFactory createConnectionFactory() throws Exception {
		ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(connector.getConnectUri());
		connectionFactory.setExceptionListener(new ExceptionListener() {
				public void onException(JMSException arg0) {
					if (arg0 instanceof ResourceAllocationException) {
						gotResourceException.set(true);
					}
				}
	        });
		return connectionFactory;
	}
}
