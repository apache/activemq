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

import java.util.Vector;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.spring.ConsumerBean;

/**
 *
 * 
 */
public class VirtualTopicPubSubTest extends EmbeddedBrokerTestSupport {

    private Vector<Connection> connections = new Vector<Connection>();
    public int ackMode = Session.AUTO_ACKNOWLEDGE;

    public static Test suite() {
        return suite(VirtualTopicPubSubTest.class);
    }

    public void initCombosForTestVirtualTopicCreation() {
        addCombinationValues("ackMode", new Object[] {new Integer(Session.AUTO_ACKNOWLEDGE), new Integer(Session.CLIENT_ACKNOWLEDGE) });
    }

    private boolean doneTwice = false;

	public void testVirtualTopicCreation() throws Exception {
	  doTestVirtualTopicCreation(10);
	}

	public void doTestVirtualTopicCreation(int total) throws Exception {

        ConsumerBean messageList = new ConsumerBean() {
            public synchronized void onMessage(Message message) {
                super.onMessage(message);
                if (ackMode == Session.CLIENT_ACKNOWLEDGE) {
                    try {
                        message.acknowledge();
                    } catch (JMSException e) {
                        e.printStackTrace();
                    }
                }

            }
        };
        messageList.setVerbose(true);

        String queueAName = getVirtualTopicConsumerName();
        // create consumer 'cluster'
        ActiveMQQueue queue1 = new ActiveMQQueue(queueAName);
        ActiveMQQueue queue2 = new ActiveMQQueue(queueAName);
  
        Session session = createStartAndTrackConnection().createSession(false, ackMode);
        MessageConsumer c1 = session.createConsumer(queue1);
         
        session = createStartAndTrackConnection().createSession(false, ackMode);
        MessageConsumer c2 = session.createConsumer(queue2);

        c1.setMessageListener(messageList);
        c2.setMessageListener(messageList);

        // create topic producer
        Session producerSession = createStartAndTrackConnection().createSession(false, ackMode);
        MessageProducer producer = producerSession.createProducer(new ActiveMQTopic(getVirtualTopicName()));
        assertNotNull(producer);

        for (int i = 0; i < total; i++) {
            producer.send(producerSession.createTextMessage("message: " + i));
        }

        messageList.assertMessagesArrived(total);

        // do twice so we confirm messages do not get redelivered after client acknowledgement
        if( doneTwice == false ) {
            doneTwice = true;
            doTestVirtualTopicCreation(0);
		}
    }

    private Connection createStartAndTrackConnection() throws Exception {
        Connection connection = createConnection();
        connection.start();
        connections.add(connection);
        return connection;
    }

    protected String getVirtualTopicName() {
        return "VirtualTopic.TEST";
    }

    protected String getVirtualTopicConsumerName() {
        return "Consumer.A.VirtualTopic.TEST";
    }


    protected void tearDown() throws Exception {
        for (Connection connection: connections) {
            connection.close();
        }
        super.tearDown();
    }
}
