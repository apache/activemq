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

import java.util.Properties;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.test.JmsTopicSendReceiveTest;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision: 1.5 $
 */
public class JmsDurableTopicSlowReceiveTest extends JmsTopicSendReceiveTest {
    
    static final int NMSG = 200;
    static final int MSIZE = 256000;
    private static final transient Log LOG = LogFactory.getLog(JmsDurableTopicSlowReceiveTest.class);
    private static final String COUNT_PROPERY_NAME = "count";

    protected Connection connection2;
    protected Session session2;
    protected Session consumeSession2;
    protected MessageConsumer consumer2;
    protected MessageProducer producer2;
    protected Destination consumerDestination2;
    BrokerService broker;
    private Connection connection3;
    private Session consumeSession3;
    private TopicSubscriber consumer3;

    /**
     * Set up a durable suscriber test.
     * 
     * @see junit.framework.TestCase#setUp()
     */
    protected void setUp() throws Exception {
        this.durable = true;
        broker = createBroker();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        broker.stop();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        ActiveMQConnectionFactory result = new ActiveMQConnectionFactory("vm://localhost?async=false");
        Properties props = new Properties();
        props.put("prefetchPolicy.durableTopicPrefetch", "5");
        props.put("prefetchPolicy.optimizeDurableTopicPrefetch", "5");
        result.setProperties(props);
        return result;
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        answer.setDeleteAllMessagesOnStartup(true);
    }

    /**
     * Test if all the messages sent are being received.
     * 
     * @throws Exception
     */
    public void testSlowReceiver() throws Exception {
        connection2 = createConnection();
        connection2.setClientID("test");
        connection2.start();
        consumeSession2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
        consumerDestination2 = session2.createTopic(getConsumerSubject() + "2");
        consumer2 = consumeSession2.createDurableSubscriber((Topic)consumerDestination2, getName());

        consumer2.close();
        connection2.close();
        new Thread(new Runnable() {

            public void run() {
                try {
                    int count = 0;
                    for (int loop = 0; loop < 4; loop++) {
                        connection2 = createConnection();
                        connection2.start();
                        session2 = connection2.createSession(false, Session.AUTO_ACKNOWLEDGE);
                        producer2 = session2.createProducer(null);
                        producer2.setDeliveryMode(deliveryMode);
                        Thread.sleep(1000);
                        for (int i = 0; i < NMSG / 4; i++) {
                            BytesMessage message = session2.createBytesMessage();
                            message.writeBytes(new byte[MSIZE]);
                            message.setStringProperty("test", "test");
                            message.setIntProperty(COUNT_PROPERY_NAME, count);
                            message.setJMSType("test");
                            producer2.send(consumerDestination2, message);
                            Thread.sleep(50);
                            if (verbose) {
                                LOG.debug("Sent(" + loop + "): " + i);
                            }
                            count++;
                        }
                        producer2.close();
                        connection2.stop();
                        connection2.close();
                    }
                } catch (Throwable e) {
                    e.printStackTrace();
                }
            }
        }, "SENDER Thread").start();
        connection3 = createConnection();
        connection3.setClientID("test");
        connection3.start();
        consumeSession3 = connection3.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        consumer3 = consumeSession3.createDurableSubscriber((Topic)consumerDestination2, getName());
        connection3.close();
        int count = 0;
        for (int loop = 0; loop < 4; ++loop) {
            connection3 = createConnection();
            connection3.setClientID("test");
            connection3.start();
            consumeSession3 = connection3.createSession(false, Session.CLIENT_ACKNOWLEDGE);
            consumer3 = consumeSession3.createDurableSubscriber((Topic)consumerDestination2, getName());
            Message msg = null;
            int i;
            for (i = 0; i < NMSG / 4; i++) {
                msg = consumer3.receive(10000);
                if (msg == null) {
                    break;
                }
                if (verbose) {
                    LOG.debug("Received(" + loop + "): " + i + " count = " + msg.getIntProperty(COUNT_PROPERY_NAME));
                }
                assertNotNull(msg);
                assertEquals(msg.getJMSType(), "test");
                assertEquals(msg.getStringProperty("test"), "test");
                assertEquals("Messages received out of order", count, msg.getIntProperty(COUNT_PROPERY_NAME));
                Thread.sleep(500);
                msg.acknowledge();
                count++;
            }
            consumer3.close();
            assertEquals("Receiver " + loop, NMSG / 4, i);
            connection3.close();
        }
    }
}
