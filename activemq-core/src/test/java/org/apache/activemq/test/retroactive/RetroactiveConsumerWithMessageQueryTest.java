/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.test.retroactive;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.MessageList;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.springframework.core.io.ClassPathResource;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import java.util.Date;

/**
 * 
 * @version $Revision$
 */
public class RetroactiveConsumerWithMessageQueryTest extends EmbeddedBrokerTestSupport {
    protected int messageCount = 20;
    protected Connection connection;
    protected Session session;

    public void testConsumeAndReceiveInitialQueryBeforeUpdates() throws Exception {

        // lets some messages
        connection = createConnection();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        connection.start();

        MessageConsumer consumer = session.createConsumer(destination);
        MessageList listener = new MessageList();
        listener.setVerbose(true);
        consumer.setMessageListener(listener);

        MessageProducer producer = session.createProducer(destination);
        int updateMessageCount = messageCount - DummyMessageQuery.messageCount;
        for (int i = 0; i < updateMessageCount; i++) {
            TextMessage message = session.createTextMessage("Update Message: " + i + " sent at: " + new Date());
            producer.send(message);
        }
        producer.close();
        System.out.println("Sent: " + updateMessageCount + " update messages");

        listener.assertMessagesReceived(messageCount);
    }

    protected void setUp() throws Exception {
        useTopic = true;
        bindAddress = "vm://localhost";
        super.setUp();
    }

    protected void tearDown() throws Exception {
        if (session != null) {
            session.close();
            session = null;
        }
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory answer = new ActiveMQConnectionFactory(bindAddress);
        answer.setUseRetroactiveConsumer(true);
        return answer;
    }

    protected BrokerService createBroker() throws Exception {
        String uri = getBrokerXml();
        System.out.println("Loading broker configuration from the classpath with URI: " + uri);
        BrokerFactoryBean factory = new BrokerFactoryBean(new ClassPathResource(uri));
        factory.afterPropertiesSet();
        return factory.getBroker();
    }

    protected void startBroker() throws Exception {
        // broker already started by XBean
    }

    protected String getBrokerXml() {
        return "org/apache/activemq/test/retroactive/activemq-message-query.xml";
    }

}
