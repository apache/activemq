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
package org.apache.activemq.plugin;

import java.net.URI;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A BrokerStatisticsPluginTest
 * A testcase for https://issues.apache.org/activemq/browse/AMQ-2379
 *
 */
public class BrokerStatisticsPluginTest extends TestCase{
    private static final Logger LOG = LoggerFactory.getLogger(BrokerStatisticsPluginTest.class);

    private Connection connection;
    private BrokerService broker;

    public void testBrokerStats() throws Exception{
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue replyTo = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(replyTo);
        Queue query = session.createQueue(StatisticsBroker.STATS_BROKER_PREFIX);
        MessageProducer producer = session.createProducer(query);
        Message msg = session.createMessage();
        msg.setJMSReplyTo(replyTo);
        producer.send(msg);
        MapMessage reply = (MapMessage) consumer.receive(10*1000);
        assertNotNull(reply);
        assertTrue(reply.getMapNames().hasMoreElements());
        assertTrue(reply.getJMSTimestamp() > 0);
        assertEquals(Message.DEFAULT_PRIORITY, reply.getJMSPriority());
        /*
        for (Enumeration e = reply.getMapNames();e.hasMoreElements();) {
            String name = e.nextElement().toString();
            System.err.println(name+"="+reply.getObject(name));
        }
        */
    }

    public void testBrokerStatsReset() throws Exception{
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue replyTo = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(replyTo);
        Queue testQueue = session.createQueue("Test.Queue");
        Queue query = session.createQueue(StatisticsBroker.STATS_BROKER_PREFIX);
        MessageProducer producer = session.createProducer(null);

        producer.send(testQueue, session.createMessage());

        Message msg = session.createMessage();
        msg.setJMSReplyTo(replyTo);
        producer.send(query, msg);
        MapMessage reply = (MapMessage) consumer.receive(10*1000);
        assertNotNull(reply);
        assertTrue(reply.getMapNames().hasMoreElements());
        assertTrue(reply.getLong("enqueueCount") >= 1);

        msg = session.createMessage();
        msg.setBooleanProperty(StatisticsBroker.STATS_BROKER_RESET_HEADER, true);
        msg.setJMSReplyTo(replyTo);
        producer.send(query, msg);
        reply = (MapMessage) consumer.receive(10*1000);
        assertNotNull(reply);
        assertTrue(reply.getMapNames().hasMoreElements());
        assertEquals(0, reply.getLong("enqueueCount"));
        assertTrue(reply.getJMSTimestamp() > 0);
        assertEquals(Message.DEFAULT_PRIORITY, reply.getJMSPriority());
    }

    public void testDestinationStats() throws Exception{
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue replyTo = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(replyTo);
        Queue testQueue = session.createQueue("Test.Queue");
        MessageProducer producer = session.createProducer(null);
        Queue query = session.createQueue(StatisticsBroker.STATS_DESTINATION_PREFIX + testQueue.getQueueName());
        Message msg = session.createMessage();

        producer.send(testQueue,msg);

        msg.setJMSReplyTo(replyTo);
        producer.send(query,msg);
        MapMessage reply = (MapMessage) consumer.receive(10 * 1000);
        assertNotNull(reply);
        assertTrue(reply.getMapNames().hasMoreElements());
        assertTrue(reply.getJMSTimestamp() > 0);
        assertEquals(Message.DEFAULT_PRIORITY, reply.getJMSPriority());
        assertTrue(reply.getLong("averageMessageSize") > 0);

        /*
        for (Enumeration e = reply.getMapNames();e.hasMoreElements();) {
            String name = e.nextElement().toString();
            System.err.println(name+"="+reply.getObject(name));
        }
        */
    }

    public void testDestinationStatsWithDot() throws Exception{
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue replyTo = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(replyTo);
        Queue testQueue = session.createQueue("Test.Queue");
        MessageProducer producer = session.createProducer(null);
        Queue query = session.createQueue(StatisticsBroker.STATS_DESTINATION_PREFIX + "." + testQueue.getQueueName());
        Message msg = session.createMessage();

        producer.send(testQueue,msg);

        msg.setJMSReplyTo(replyTo);
        producer.send(query,msg);
        MapMessage reply = (MapMessage) consumer.receive(10 * 1000);
        assertNotNull(reply);
        assertTrue(reply.getMapNames().hasMoreElements());
        assertTrue(reply.getJMSTimestamp() > 0);
        assertEquals(Message.DEFAULT_PRIORITY, reply.getJMSPriority());
        /*
        for (Enumeration e = reply.getMapNames();e.hasMoreElements();) {
            String name = e.nextElement().toString();
            System.err.println(name+"="+reply.getObject(name));
        }
        */
    }

    public void testDestinationStatsWithNullTermination() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue replyTo = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(replyTo);
        Queue testQueue = session.createQueue("Test.Queue");
        MessageProducer producer = session.createProducer(null);
        Queue query = session.createQueue(StatisticsBroker.STATS_DESTINATION_PREFIX + "." + testQueue.getQueueName());
        Message msg = session.createMessage();
        // Instruct to terminate query reply with a null-message
        msg.setBooleanProperty(StatisticsBroker.STATS_DENOTE_END_LIST, true);

        producer.send(testQueue, msg);

        msg.setJMSReplyTo(replyTo);
        producer.send(query, msg);
        MapMessage reply = (MapMessage) consumer.receive(10 * 1000);
        assertNotNull(reply);
        assertTrue(reply.getMapNames().hasMoreElements());
        assertEquals(1, reply.getLong("size"));
        assertTrue(reply.getJMSTimestamp() > 0);
        assertEquals(Message.DEFAULT_PRIORITY, reply.getJMSPriority());

        /*
        for (Enumeration e = reply.getMapNames(); e.hasMoreElements();) {
            String name = e.nextElement().toString();
            System.err.println(name+"="+reply.getObject(name));
        }
         */

        // Assert that we got a null-termination
        MapMessage nullReply = (MapMessage) consumer.receive(10 * 1000);
        assertNotNull(nullReply);
        // No props in null-message
        assertFalse(nullReply.getMapNames().hasMoreElements());
        assertTrue(nullReply.getJMSTimestamp() > 0);
        assertEquals(Message.DEFAULT_PRIORITY, nullReply.getJMSPriority());
    }

    public void testDestinationStatsWithFirstMessageTimestamp() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue replyTo = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(replyTo);
        Queue testQueue = session.createQueue("Test.Queue");
        MessageProducer producer = session.createProducer(null);
        Queue query = session.createQueue(StatisticsBroker.STATS_DESTINATION_PREFIX + "." + testQueue.getQueueName());
        Message msg = session.createMessage();
        // Instruct to include timestamp of first message in the queue
        msg.setBooleanProperty(StatisticsBroker.STATS_FIRST_MESSAGE_TIMESTAMP, true);

        producer.send(testQueue, msg);

        msg.setJMSReplyTo(replyTo);
        producer.send(query, msg);
        MapMessage reply = (MapMessage) consumer.receive(10 * 1000);
        assertNotNull(reply);
        assertTrue(reply.getMapNames().hasMoreElements());
        assertEquals(1, reply.getLong("size"));
        assertTrue(reply.getJMSTimestamp() > 0);
        // Assert that we got the brokerInTime for the first message in queue as value of key "firstMessageTimestamp"
        assertTrue(System.currentTimeMillis() >= reply.getLong("firstMessageTimestamp"));
        assertEquals(Message.DEFAULT_PRIORITY, reply.getJMSPriority());

        /*
        for (Enumeration e = reply.getMapNames(); e.hasMoreElements();) {
            String name = e.nextElement().toString();
            System.err.println(name+"="+reply.getObject(name));
        }
         */
    }

    @SuppressWarnings("unused")
    public void testSubscriptionStats() throws Exception{
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue replyTo = session.createTemporaryQueue();
        MessageConsumer consumer = session.createConsumer(replyTo);
        Queue testQueue = session.createQueue("Test.Queue");
        MessageConsumer testConsumer = session.createConsumer(testQueue);
        MessageProducer producer = session.createProducer(null);
        Queue query = session.createQueue(StatisticsBroker.STATS_SUBSCRIPTION_PREFIX);
        Message msg = session.createMessage();

        producer.send(testQueue,msg);

        msg.setJMSReplyTo(replyTo);
        producer.send(query,msg);
        MapMessage reply = (MapMessage) consumer.receive(10 * 1000);
        assertNotNull(reply);
        assertTrue(reply.getMapNames().hasMoreElements());
        assertTrue(reply.getJMSTimestamp() > 0);
        assertEquals(Message.DEFAULT_PRIORITY, reply.getJMSPriority());

        /*for (Enumeration e = reply.getMapNames();e.hasMoreElements();) {
            String name = e.nextElement().toString();
            System.err.println(name+"="+reply.getObject(name));
        }*/
    }

    @Override
    protected void setUp() throws Exception {
        broker = createBroker();
        ConnectionFactory factory = new ActiveMQConnectionFactory(broker.getTransportConnectorURIsAsMap().get("tcp"));
        connection = factory.createConnection();
        connection.start();
    }

    @Override
    protected void tearDown() throws Exception{
        if (this.connection != null) {
            this.connection.close();
        }
        if (this.broker!=null) {
            this.broker.stop();
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        BrokerPlugin[] plugins = new BrokerPlugin[1];
        plugins[0] = new StatisticsBrokerPlugin();
        answer.setPlugins(plugins);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector("tcp://localhost:0");
        answer.start();
        return answer;
    }

    protected BrokerService createBroker(String uri) throws Exception {
        LOG.info("Loading broker configuration from the classpath with URI: " + uri);
        return BrokerFactory.createBroker(new URI("xbean:" + uri));
    }
}
