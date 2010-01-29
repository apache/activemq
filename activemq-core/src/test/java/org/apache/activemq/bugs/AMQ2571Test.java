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

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;
import javax.jms.TextMessage;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;

public class AMQ2571Test extends EmbeddedBrokerTestSupport {

    public void testTempQueueClosing() {
        try {
            ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(this.bindAddress);

            // First create session that will own the TempQueue
            Connection connectionA = connectionFactory.createConnection();
            connectionA.start();

            Session sessionA = connectionA.createSession(false, Session.AUTO_ACKNOWLEDGE);

            TemporaryQueue tempQueue = sessionA.createTemporaryQueue();

            // Next, create session that will put messages on the queue.
            Connection connectionB = connectionFactory.createConnection();
            connectionB.start();

            Session sessionB = connectionB.createSession(false, Session.AUTO_ACKNOWLEDGE);

            // Create a producer for connection B.
            final MessageProducer producerB = sessionB.createProducer(tempQueue);
            producerB.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

            final TextMessage message = sessionB.createTextMessage("Testing AMQ TempQueue.");

            Thread sendingThread = new Thread(new Runnable() {
                public void run() {
                    try {
                        for (int i = 0; i < 5000; i++) {
                            producerB.send(message);
                        }
                    } catch (JMSException e) {
                        // We don't get this exception every time.
                        // Not getting it means that we don't know if the
                        // creator of the TempQueue has disconnected.
                    }
                }
            });

            // Send 5000 messages.
            sendingThread.start();
            // Now close connection A. This will remove the TempQueue.
            connectionA.close();
            // Wait for the thread to finish.
            sendingThread.join();

            // Sleep for a while to make sure that we should know that the
            // TempQueue is gone.
            Thread.sleep(5000);

            // Now we test if we are able to send again.
            try {
                producerB.send(message);
                fail("Involuntary recreated temporary queue.");
            } catch (JMSException e) {
                // Got exception, just as we wanted because the creator of
                // the TempQueue had closed the connection prior to the send.
                assertTrue("TempQueue does not exist anymore.", true);
            }
        } catch (Exception e) {
            fail("Unexpected exception " + e);
        }
    }

    @Override
    protected void setUp() throws Exception {
        bindAddress = "vm://localhost";
        super.setUp();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setPersistent(false);
        answer.setUseJmx(false);
        answer.addConnector(bindAddress);
        return answer;
    }
}