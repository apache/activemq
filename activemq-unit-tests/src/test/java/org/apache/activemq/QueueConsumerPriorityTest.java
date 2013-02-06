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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.command.ActiveMQQueue;

public class QueueConsumerPriorityTest extends TestCase {

    private static final String VM_BROKER_URL = "vm://localhost?broker.persistent=false&broker.useJmx=true";

    public QueueConsumerPriorityTest(String name) {
        super(name);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private Connection createConnection(final boolean start) throws JMSException {
        ConnectionFactory cf = new ActiveMQConnectionFactory(VM_BROKER_URL);
        Connection conn = cf.createConnection();
        if (start) {
            conn.start();
        }
        return conn;
    }

    public void testQueueConsumerPriority() throws JMSException, InterruptedException {
        Connection conn = createConnection(true);

        Session consumerLowPriority = null;
        Session consumerHighPriority = null;
        Session senderSession = null;

        try {

            consumerLowPriority = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            consumerHighPriority = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            assertNotNull(consumerHighPriority);
            senderSession = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
            String queueName = getClass().getName();
            ActiveMQQueue low = new ActiveMQQueue(queueName+"?consumer.priority=1");
            MessageConsumer lowConsumer = consumerLowPriority.createConsumer(low);

            ActiveMQQueue high = new ActiveMQQueue(queueName+"?consumer.priority=2");
            MessageConsumer highConsumer = consumerLowPriority.createConsumer(high);

            ActiveMQQueue senderQueue = new ActiveMQQueue(queueName);

            MessageProducer producer = senderSession.createProducer(senderQueue);

            Message msg = senderSession.createTextMessage("test");
            for (int i =0; i< 10000;i++) {
                producer.send(msg);
                assertNotNull("null on iteration: " + i, highConsumer.receive(500));
            }
            assertNull(lowConsumer.receive(2000));

        } finally {
            conn.close();
        }
    }
}

