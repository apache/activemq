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
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

/**
 * @version $Revision: 1.1.1.1 $
 */
public class ConsumerReceiveWithTimeoutTest extends TestSupport {

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
     * Test to check if consumer thread wakes up inside a receive(timeout) after
     * a message is dispatched to the consumer
     * 
     * @throws javax.jms.JMSException
     */
    public void testConsumerReceiveBeforeMessageDispatched() throws JMSException {

        connection.start();

        final Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        final Queue queue = session.createQueue("test");

        Thread t = new Thread() {
            public void run() {
                try {
                    // wait for 10 seconds to allow consumer.receive to be run
                    // first
                    Thread.sleep(10000);
                    MessageProducer producer = session.createProducer(queue);
                    producer.send(session.createTextMessage("Hello"));
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        t.start();

        // Consume the message...
        MessageConsumer consumer = session.createConsumer(queue);
        Message msg = consumer.receive(60000);
        assertNotNull(msg);
        session.close();

    }

}
