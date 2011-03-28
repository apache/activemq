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
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * 
 */
public class JmsAutoAckListenerTest extends TestSupport implements MessageListener {

    private Connection connection;

    protected void setUp() throws Exception {
        super.setUp();
        connection = createConnection();
    }

    /**
     * @see junit.framework.TestCase#tearDown()
     */
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
            connection = null;
        }
        super.tearDown();
    }

    /**
     * Tests if acknowleged messages are being consumed.
     * 
     * @throws javax.jms.JMSException
     */
    public void testAckedMessageAreConsumed() throws Exception {
        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test");
        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello"));

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        consumer.setMessageListener(this);

        Thread.sleep(10000);
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        // Attempt to Consume the message...check if message was acknowledge
        consumer = session.createConsumer(queue);
        Message msg = consumer.receive(1000);
        assertNull(msg);

        session.close();
    }

    public void onMessage(Message message) {
        assertNotNull(message);

    }
}
