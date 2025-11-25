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
import jakarta.jms.Connection;
import jakarta.jms.Message;
import jakarta.jms.MessageListener;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TextMessage;
import jakarta.jms.Topic;
import jakarta.jms.TopicSubscriber;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.experimental.categories.Category;

/**
 * 
 */
@Category(ParallelTest.class)
public class JMSDurableTopicNoLocalTest extends EmbeddedBrokerTestSupport {
    protected String bindAddress;

    public void testConsumeNoLocal() throws Exception {
        final String TEST_NAME = getClass().getName();
        Connection connection = createConnection();
        connection.setClientID(TEST_NAME);
        
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        
        TopicSubscriber subscriber = session.createDurableSubscriber((Topic) destination, "topicUser2", null, true);
        
        
        final CountDownLatch latch = new CountDownLatch(1);
        subscriber.setMessageListener(new MessageListener() {
            public void onMessage(Message message) {
                System.out.println("Receive a message " + message);
                latch.countDown();        
            }   
        });
        
        connection.start();
        
        MessageProducer producer = session.createProducer(destination);
        TextMessage message = session.createTextMessage("THIS IS A TEST");
        producer.send(message);
        producer.close();
        latch.await(5,TimeUnit.SECONDS);
        assertEquals(latch.getCount(),1);
    }

    @Override
    protected void setUp() throws Exception {
        bindAddress = "vm://localhost";
        useTopic=true;
        super.setUp();
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setUseJmx(false);
        answer.setPersistent(true);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector(bindAddress);
        return answer;
    }

}
