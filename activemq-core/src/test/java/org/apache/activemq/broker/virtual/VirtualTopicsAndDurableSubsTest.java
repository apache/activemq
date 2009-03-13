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
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.broker.jmx.MBeanTest;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.spring.ConsumerBean;

public class VirtualTopicsAndDurableSubsTest extends MBeanTest {

    private Connection connection;

    public void testVirtualTopicCreationAndDurableSubs() throws Exception {
        if (connection == null) {
            connection = createConnection();
        }
        connection.setClientID(getAClientID());
        connection.start();

        ConsumerBean messageList = new ConsumerBean();
        messageList.setVerbose(true);
        
        String queueAName = getVirtualTopicConsumerName();
        // create consumer 'cluster'
        ActiveMQQueue queue1 = new ActiveMQQueue(queueAName);
        ActiveMQQueue queue2 = new ActiveMQQueue(queueAName);

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer c1 = session.createConsumer(queue1);
        MessageConsumer c2 = session.createConsumer(queue2);

        c1.setMessageListener(messageList);
        c2.setMessageListener(messageList);

        // create topic producer
        MessageProducer producer = session.createProducer(new ActiveMQTopic(getVirtualTopicName()));
        assertNotNull(producer);

        int total = 10;
        for (int i = 0; i < total; i++) {
            producer.send(session.createTextMessage("message: " + i));
        }
        messageList.assertMessagesArrived(total);
        
        //Add and remove durable subscriber after using VirtualTopics
        assertCreateAndDestroyDurableSubscriptions();
    }

    protected String getAClientID(){
    	return "VirtualTopicCreationAndDurableSubs";
    }

    protected String getVirtualTopicName() {
        return "VirtualTopic.TEST";
    }


    protected String getVirtualTopicConsumerName() {
        return "Consumer.A.VirtualTopic.TEST";
    }

    protected String getDurableSubscriberName(){
    	return "Sub1";
    }
    
    protected String getDurableSubscriberTopicName(){
    	return "simple.topic";
    }

    protected void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }
    
    //Overrides test cases from MBeanTest to avoid having them run.
    public void testMBeans() throws Exception {}
    public void testMoveMessages() throws Exception {}
    public void testRetryMessages() throws Exception {}
    public void testMoveMessagesBySelector() throws Exception {}
    public void testCopyMessagesBySelector() throws Exception {}
}
