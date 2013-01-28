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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQMessageConsumer;
import org.apache.activemq.JmsMultipleBrokersTestSupport;
import org.apache.activemq.command.ActiveMQQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

import javax.jms.ConnectionFactory;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TemporaryQueue;

public class TwoSecureBrokerRequestReplyTest extends JmsMultipleBrokersTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(TwoSecureBrokerRequestReplyTest.class);

    public void setUp() throws Exception {
        super.setAutoFail(true);
        super.setUp();

        createBroker(new ClassPathResource("org/apache/activemq/usecases/sender-secured.xml"));
        createBroker(new ClassPathResource("org/apache/activemq/usecases/receiver-secured.xml"));
    }

    public void testRequestReply() throws Exception {
        ActiveMQQueue requestReplyDest = new ActiveMQQueue("RequestReply");

        startAllBrokers();
        waitForBridgeFormation();
        waitForMinTopicRegionConsumerCount("sender", 1);
        waitForMinTopicRegionConsumerCount("receiver", 1);


        ConnectionFactory factory = getConnectionFactory("sender");
        ActiveMQConnection conn = (ActiveMQConnection) factory.createConnection("system", "manager");
        conn.setWatchTopicAdvisories(false);
        conn.start();
        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ConnectionFactory replyFactory = getConnectionFactory("receiver");
        for (int i = 0; i < 2000; i++) {
            TemporaryQueue tempDest = session.createTemporaryQueue();
            MessageProducer producer = session.createProducer(requestReplyDest);
            javax.jms.Message message = session.createTextMessage("req-" + i);
            message.setJMSReplyTo(tempDest);

            ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(tempDest);
            producer.send(message);

            ActiveMQConnection replyConnection = (ActiveMQConnection) replyFactory.createConnection("system", "manager");
            replyConnection.setWatchTopicAdvisories(false);
            replyConnection.start();
            Session replySession = replyConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            ActiveMQMessageConsumer replyConsumer = (ActiveMQMessageConsumer) replySession.createConsumer(requestReplyDest);
            javax.jms.Message msg = replyConsumer.receive(10000);
            assertNotNull("request message not null: " + i, msg);
            MessageProducer replyProducer = replySession.createProducer(msg.getJMSReplyTo());
            replyProducer.send(session.createTextMessage("reply-" + i));
            replyConnection.close();

            javax.jms.Message reply = consumer.receive(10000);
            assertNotNull("reply message : " + i + ", to: " + tempDest + ", by consumer:" + consumer.getConsumerId(), reply);
            consumer.close();
            tempDest.delete();
            LOG.info("message #" + i + " processed");
        }

    }


}
