/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.test.rollback;

import edu.emory.mathcs.backport.java.util.concurrent.CountDownLatch;
import edu.emory.mathcs.backport.java.util.concurrent.TimeUnit;
import edu.emory.mathcs.backport.java.util.concurrent.atomic.AtomicInteger;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.springframework.jms.core.MessageCreator;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @version $Revision$
 */
public class RollbacksWhileConsumingLargeQueueTest extends EmbeddedBrokerTestSupport implements MessageListener {

    protected int numberOfMessagesOnQueue = 6500;
    private Connection connection;
    private DelegatingTransactionalMessageListener messageListener;
    private AtomicInteger counter = new AtomicInteger(0);
    private CountDownLatch latch;

    public void testConsumeOnFullQueue() throws Exception {
        boolean answer = latch.await(1000, TimeUnit.SECONDS);

        System.out.println("Received: " + counter.get() + "  message(s)");
        assertTrue("Did not receive the latch!", answer);
    }


    protected void setUp() throws Exception {
        super.setUp();

        connection = createConnection();
        connection.start();

        // lets fill the queue up
        for (int i = 0; i < numberOfMessagesOnQueue; i++) {
            template.send(createMessageCreator(i));
        }

        latch = new CountDownLatch(numberOfMessagesOnQueue);
        messageListener = new DelegatingTransactionalMessageListener(this, connection, destination);
    }


    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

    protected MessageCreator createMessageCreator(final int i) {
        return new MessageCreator() {
            public Message createMessage(Session session) throws JMSException {
                TextMessage answer = session.createTextMessage("Message: " + i);
                answer.setIntProperty("Counter", i);
                return answer;
            }
        };
    }

    public void onMessage(Message message) {
        int value = counter.incrementAndGet();
        if (value % 10 == 0) {
            throw new RuntimeException("Dummy exception on message: " + value);
        }

        log.info("Received message: " + value + " content: " + message);

        latch.countDown();
    }
}
