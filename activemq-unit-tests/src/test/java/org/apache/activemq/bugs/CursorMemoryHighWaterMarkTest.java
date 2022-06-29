/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.Connection;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * ensure a message will be pages in from the store when another dest has stopped caching
 */

public class CursorMemoryHighWaterMarkTest {
    private static final Logger LOG = LoggerFactory
            .getLogger(CursorMemoryHighWaterMarkTest.class);


    public static final String MY_QUEUE_2 = "myQueue_2";
    public static final String MY_QUEUE = "myQueue";
    public static final String BROKER_NAME = "myBroker";
    private BrokerService broker1;
    private ActiveMQConnectionFactory connectionFactory;

    @Before
    public void setUp() throws Exception {

        broker1 = createAndStartBroker(BROKER_NAME);
        broker1.waitUntilStarted();

        connectionFactory = new ActiveMQConnectionFactory("vm://" + BROKER_NAME);
    }


    private BrokerService createAndStartBroker(String name)
            throws Exception {
        BrokerService broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setBrokerName(name);
        broker.setUseJmx(true);

        broker.getSystemUsage().getMemoryUsage().setLimit(5000000l);

        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();

        //1 mb limit
        policy.setMemoryLimit(1024000);

        policy.setCursorMemoryHighWaterMark(50);

        policyMap.put(new ActiveMQQueue(MY_QUEUE_2), policy);
        broker.setDestinationPolicy(policyMap);

        broker.start();

        return broker;
    }

    @After
    public void tearDown() throws Exception {
        broker1.stop();
    }


    @Test
    public void testCursorHighWaterMark() throws Exception {

        //check the memory usage on broker1 (source broker ) has returned to zero
        int systemUsage = broker1.getSystemUsage().getMemoryUsage().getPercentUsage();
        assertEquals("System Usage on broker1 before test", 0, systemUsage);

        //produce message
        produceMesssages(MY_QUEUE, 3000);

        //verify usage is greater than 60%
        systemUsage = broker1.getSystemUsage().getMemoryUsage().getPercentUsage();
        assertTrue("System Usage on broker1 before test", 60 < systemUsage);

        LOG.info("Broker System Mem Usage: " + broker1.getSystemUsage().getMemoryUsage());

        //send a mesage to myqueue.2
        produceMesssages(MY_QUEUE_2, 1);

        //try to consume that message
        consume(MY_QUEUE_2, 1);

    }


    private void produceMesssages(String queue, int messageCount) throws Exception {

        Connection con = connectionFactory.createConnection();
        try {
            con.start();
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer producer = session.createProducer(session.createQueue(queue));

            String textMessage = getTextForMessage();
            TextMessage msg = session.createTextMessage(textMessage);

            for (int i = 0; i < messageCount; i++) {
                producer.send(msg);
            }

        } finally {
            con.close();
        }

    }


    private String getTextForMessage() {
        StringBuffer stringBuffer = new StringBuffer();

        for (int i = 0; i > 10000; i++) {
            stringBuffer.append("0123456789");
        }

        return stringBuffer.toString();
    }


    private void consume(String queue, int messageCount) throws Exception {

        Connection con = connectionFactory.createConnection();
        try {
            con.start();
            Session session = con.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer messageConsumer
                    = session.createConsumer(session.createQueue(queue));

            for (int i = 0; i < messageCount; i++) {
                javax.jms.Message message = messageConsumer.receive(5000l);
                if (message == null) {
                    fail("should have received a message");
                }
            }

        } finally {
            con.close();
        }

    }

}