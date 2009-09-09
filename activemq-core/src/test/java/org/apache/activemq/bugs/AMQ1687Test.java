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

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.spring.ConsumerBean;

/**
 * 
 * @version $Revision: $
 */
public class AMQ1687Test extends EmbeddedBrokerTestSupport {

    private Connection connection;

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        //prefetch change is not required, but test will not fail w/o it, only spew errors in the AMQ log.
        return new ActiveMQConnectionFactory(this.bindAddress+"?jms.prefetchPolicy.all=5");
        //return super.createConnectionFactory();
        //return new ActiveMQConnectionFactory("tcp://localhost:61616");
    }
    
    public void testVirtualTopicCreation() throws Exception {
        if (connection == null) {
            connection = createConnection();
        }
        connection.start();

        ConsumerBean messageList = new ConsumerBean();
        messageList.setVerbose(true);
        
        String queueAName = getVirtualTopicConsumerName();
        String queueBName = getVirtualTopicConsumerNameB();
        
        // create consumer 'cluster'
        ActiveMQQueue queue1 = new ActiveMQQueue(queueAName);
        ActiveMQQueue queue2 = new ActiveMQQueue(queueBName);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer c1 = session.createConsumer(queue1);
        MessageConsumer c2 = session.createConsumer(queue2);

        c1.setMessageListener(messageList);
        c2.setMessageListener(messageList);

        // create topic producer
        ActiveMQTopic topic = new ActiveMQTopic(getVirtualTopicName());
        MessageProducer producer = session.createProducer(topic);
        assertNotNull(producer);

        int total = 100;
        for (int i = 0; i < total; i++) {
            producer.send(session.createTextMessage("message: " + i));
        }
        
        messageList.assertMessagesArrived(total*2);
    }


    protected String getVirtualTopicName() {
        return "VirtualTopic.TEST";
    }


    protected String getVirtualTopicConsumerName() {
        return "Consumer.A.VirtualTopic.TEST";
    }

    protected String getVirtualTopicConsumerNameB() {
        return "Consumer.B.VirtualTopic.TEST";
    }
    
    
    protected void setUp() throws Exception {
        this.bindAddress="tcp://localhost:61616";
        super.setUp();
    }
    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }
}