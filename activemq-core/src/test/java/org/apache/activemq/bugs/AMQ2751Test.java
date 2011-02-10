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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ2751Test extends EmbeddedBrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(AMQ2751Test.class);

    private static String clientIdPrefix = "consumer";
    private static String queueName = "FOO";

    public void testRecoverRedelivery() throws Exception {

        final CountDownLatch redelivery = new CountDownLatch(6);
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("failover:(" + this.bindAddress + ")");
        try {

            Connection connection = factory.createConnection();
            String clientId = clientIdPrefix;
            connection.setClientID(clientId);

            final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);

            Queue queue = session.createQueue(queueName);

            MessageConsumer consumer = session.createConsumer(queue);

            consumer.setMessageListener(new MessageListener() {
                public void onMessage(Message message) {
                    try {
                        LOG.info("Got message: " + message.getJMSMessageID());
                        if (message.getJMSRedelivered()) {
                            LOG.info("It's a redelivery.");
                            redelivery.countDown();
                        } 
                        LOG.info("calling recover() on the session to force redelivery.");
                        session.recover();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }
            });

            System.out.println("Created queue consumer with clientId " + clientId);
            connection.start();

            MessageProducer producer = session.createProducer(queue);
            producer.send(session.createTextMessage("test"));
            
            assertTrue("we got 6 redeliveries", redelivery.await(20, TimeUnit.SECONDS));

        } finally {
            broker.stop();
        }

    }

    @Override
    protected void setUp() throws Exception {
        bindAddress = "tcp://0.0.0.0:61617";
        super.setUp();
    }
}
