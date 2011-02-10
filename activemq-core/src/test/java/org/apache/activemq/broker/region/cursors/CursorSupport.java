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
package org.apache.activemq.broker.region.cursors;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

/**
 * @version $Revision: 1.3 $
 */
public abstract class CursorSupport extends CombinationTestSupport {

    public int MESSAGE_COUNT = 500;
    public int PREFETCH_SIZE = 50;
    private static final Logger LOG = LoggerFactory.getLogger(CursorSupport.class);

    protected BrokerService broker;
    protected String bindAddress = "tcp://localhost:60706";

    protected abstract Destination getDestination(Session session) throws JMSException;

    protected abstract MessageConsumer getConsumer(Connection connection) throws Exception;

    protected abstract void configureBroker(BrokerService answer) throws Exception;

    public void testSendFirstThenConsume() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        Connection consumerConnection = getConsumerConnection(factory);
        MessageConsumer consumer = getConsumer(consumerConnection);
        consumerConnection.close();
        Connection producerConnection = factory.createConnection();
        producerConnection.start();
        Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(getDestination(session));
        List<Message> senderList = new ArrayList<Message>();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = session.createTextMessage("test" + i);
            senderList.add(msg);
            producer.send(msg);
        }
        producerConnection.close();
        // now consume the messages
        consumerConnection = getConsumerConnection(factory);
        // create durable subs
        consumer = getConsumer(consumerConnection);
        List<Message> consumerList = new ArrayList<Message>();
        for (int i = 0; i < MESSAGE_COUNT; i++) {
            Message msg = consumer.receive(1000*5);
            assertNotNull("Message "+i+" was missing.", msg);
            consumerList.add(msg);
        }
        assertEquals(senderList, consumerList);
        consumerConnection.close();
    }


    public void initCombosForTestSendWhilstConsume() {
        addCombinationValues("MESSAGE_COUNT", new Object[] {Integer.valueOf(400),
                                                           Integer.valueOf(500)});
        addCombinationValues("PREFETCH_SIZE", new Object[] {Integer.valueOf(100),
                Integer.valueOf(50)});
    }

    public void testSendWhilstConsume() throws Exception {
        ConnectionFactory factory = createConnectionFactory();
        Connection consumerConnection = getConsumerConnection(factory);
        // create durable subs
        MessageConsumer consumer = getConsumer(consumerConnection);
        consumerConnection.close();
        Connection producerConnection = factory.createConnection();
        producerConnection.start();
        Session session = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(getDestination(session));
        List<TextMessage> senderList = new ArrayList<TextMessage>();
        for (int i = 0; i < MESSAGE_COUNT / 10; i++) {
            TextMessage msg = session.createTextMessage("test" + i);
            senderList.add(msg);
            producer.send(msg);
        }
        // now consume the messages
        consumerConnection = getConsumerConnection(factory);
        // create durable subs
        consumer = getConsumer(consumerConnection);
        final List<Message> consumerList = new ArrayList<Message>();
        final CountDownLatch latch = new CountDownLatch(1);
        consumer.setMessageListener(new MessageListener() {

            public void onMessage(Message msg) {
                try {
                    // sleep to act as a slow consumer
                    // which will force a mix of direct and polled dispatching
                    // using the cursor on the broker
                    Thread.sleep(50);
                } catch (Exception e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                consumerList.add(msg);
                if (consumerList.size() == MESSAGE_COUNT) {
                    latch.countDown();
                }
            }
        });
        for (int i = MESSAGE_COUNT / 10; i < MESSAGE_COUNT; i++) {
            TextMessage msg = session.createTextMessage("test" + i);
            senderList.add(msg);
            producer.send(msg);
        }
        latch.await(300000, TimeUnit.MILLISECONDS);
        producerConnection.close();
        consumerConnection.close();
        assertEquals("Still dipatching - count down latch not sprung", latch.getCount(), 0);
        //assertEquals("cosumerList - expected: " + MESSAGE_COUNT + " but was: " + consumerList.size(), consumerList.size(), senderList.size());
        for (int i = 0; i < senderList.size(); i++) {
            Message sent = senderList.get(i);
            Message consumed = consumerList.get(i);
            if (!sent.equals(consumed)) {
                LOG.error("BAD MATCH AT POS " + i);
                LOG.error(sent.toString());
                LOG.error(consumed.toString());
                /*
                 * log.error("\n\n\n\n\n"); for (int j = 0; j <
                 * consumerList.size(); j++) { log.error(consumerList.get(j)); }
                 */
            }
            assertEquals("This should be the same at pos " + i + " in the list", sent.getJMSMessageID(), consumed.getJMSMessageID());
        }
    }
    
    protected Connection getConsumerConnection(ConnectionFactory fac) throws JMSException {
        Connection connection = fac.createConnection();
        connection.setClientID("testConsumer");
        connection.start();
        return connection;
    }

    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(bindAddress);
        Properties props = new Properties();
        props.setProperty("prefetchPolicy.durableTopicPrefetch", "" + PREFETCH_SIZE);
        props.setProperty("prefetchPolicy.optimizeDurableTopicPrefetch", "" + PREFETCH_SIZE);
        props.setProperty("prefetchPolicy.queuePrefetch", "" + PREFETCH_SIZE);
        cf.setProperties(props);
        return cf;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }
}
