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
package org.apache.activemq.broker.virtual;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.spring.ConsumerBean;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: $
 */
public class MirroredQueueTest extends EmbeddedBrokerTestSupport {
    private static final transient Log LOG = LogFactory.getLog(MirroredQueueTest.class);
    private Connection connection;

    public void testSendingToQueueIsMirrored() throws Exception {
        if (connection == null) {
            connection = createConnection();
        }
        connection.start();

        ConsumerBean messageList = new ConsumerBean();
        messageList.setVerbose(true);

        Destination consumeDestination = createConsumeDestination();

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        LOG.info("Consuming from: " + consumeDestination);

        MessageConsumer c1 = session.createConsumer(consumeDestination);
        c1.setMessageListener(messageList);

        // create topic producer
        ActiveMQQueue sendDestination = new ActiveMQQueue(getQueueName());
        LOG.info("Sending to: " + sendDestination);

        MessageProducer producer = session.createProducer(sendDestination);
        assertNotNull(producer);

        int total = 10;
        for (int i = 0; i < total; i++) {
            producer.send(session.createTextMessage("message: " + i));
        }

        ///Thread.sleep(1000000);

        messageList.assertMessagesArrived(total);

        LOG.info("Received: " + messageList);
        System.out.println("Received: " + messageList.flushMessages());
    }

    protected Destination createConsumeDestination() {
        return new ActiveMQTopic("VirtualTopic.Mirror." + getQueueName());
    }

    protected String getQueueName() {
        return "My.Queue";
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseMirroredQueues(true);
        answer.setPersistent(isPersistent());
        answer.addConnector(bindAddress);
        return answer;
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }
}