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

import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.command.ActiveMQDestination;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchedMessagePriorityConsumerTest extends JmsTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(BatchedMessagePriorityConsumerTest.class);

    public void testBatchWithLowPriorityFirstAndClientSupport() throws Exception {
        doTestBatchWithLowPriorityFirst(true);
    }

    public void testBatchWithLowPriorityFirstAndClientSupportOff() throws Exception {
        doTestBatchWithLowPriorityFirst(false);
    }

    public void doTestBatchWithLowPriorityFirst(boolean clientPrioritySupport) throws Exception {

        connection.start();
        connection.setMessagePrioritySupported(clientPrioritySupport);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        ActiveMQDestination destination = createDestination(session, ActiveMQDestination.QUEUE_TYPE);


        MessageProducer producer = session.createProducer(destination);
        producer.setPriority(0);
        sendMessages(session, producer, 2);
        producer.close();

        MessageProducer producer2 = session.createProducer(destination);
        producer2.setPriority(9);
        sendMessages(session, producer2, 3);
        producer2.close();

        session.close();

        Session consumerSession = connection.createSession(true, Session.SESSION_TRANSACTED);
        MessageConsumer messageConsumer = consumerSession.createConsumer(destination);

        for (int i = 0; i < 5; i++) {
            Message message = messageConsumer.receive(4000);
            LOG.info("MessageID: " + message.getJMSMessageID());
        }

        consumerSession.commit();
        consumerSession.close();

        // should be nothing left
        consumerSession = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        messageConsumer = consumerSession.createConsumer(destination);

        assertNull("No message left", messageConsumer.receive(1000));

        consumerSession.close();

    }
}