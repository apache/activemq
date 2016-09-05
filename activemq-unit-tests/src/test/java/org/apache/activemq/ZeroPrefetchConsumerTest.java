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
package org.apache.activemq;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ConsumerControl;
import org.apache.activemq.command.ExceptionResponse;
import org.apache.activemq.spring.SpringConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class ZeroPrefetchConsumerTest extends EmbeddedBrokerTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(ZeroPrefetchConsumerTest.class);

    protected Connection connection;
    protected Queue queue;
    protected Queue brokerZeroQueue = new ActiveMQQueue("brokerZeroConfig");

    public void testCannotUseMessageListener() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = session.createConsumer(queue);

        MessageListener listener = new SpringConsumer();
        try {
            consumer.setMessageListener(listener);
            fail("Should have thrown JMSException as we cannot use MessageListener with zero prefetch");
        } catch (JMSException e) {
            LOG.info("Received expected exception : " + e);
        }
    }

    public void testPullConsumerWorks() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Hello World!"));

        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);
        Message answer = consumer.receive(5000);
        assertNotNull("Should have received a message!", answer);
        // check if method will return at all and will return a null
        answer = consumer.receive(1);
        assertNull("Should have not received a message!", answer);
        answer = consumer.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    public void testIdleConsumer() throws Exception {
        doTestIdleConsumer(false);
    }

    public void testIdleConsumerTranscated() throws Exception {
        doTestIdleConsumer(true);
    }

    private void doTestIdleConsumer(boolean transacted) throws Exception {
        Session session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));
        producer.send(session.createTextMessage("Msg2"));
        if (transacted) {
            session.commit();
        }
        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);

        session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg1");
        if (transacted) {
            session.commit();
        }
        // this call would return null if prefetchSize > 0
        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg2");
        if (transacted) {
            session.commit();
        }
        answer = (TextMessage)consumer.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    public void testRecvRecvCommit() throws Exception {
        doTestRecvRecvCommit(false);
    }

    public void testRecvRecvCommitTranscated() throws Exception {
        doTestRecvRecvCommit(true);
    }

    private void doTestRecvRecvCommit(boolean transacted) throws Exception {
        Session session = connection.createSession(transacted, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));
        producer.send(session.createTextMessage("Msg2"));
        if (transacted) {
            session.commit();
        }
        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer.receiveNoWait();
        assertEquals("Should have received a message!", answer.getText(), "Msg1");
        answer = (TextMessage)consumer.receiveNoWait();
        assertEquals("Should have received a message!", answer.getText(), "Msg2");
        if (transacted) {
            session.commit();
        }
        answer = (TextMessage)consumer.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    public void testTwoConsumers() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));
        producer.send(session.createTextMessage("Msg2"));

        // now lets receive it
        MessageConsumer consumer1 = session.createConsumer(queue);
        MessageConsumer consumer2 = session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer1.receiveNoWait();
        assertEquals("Should have received a message!", answer.getText(), "Msg1");
        answer = (TextMessage)consumer2.receiveNoWait();
        assertEquals("Should have received a message!", answer.getText(), "Msg2");

        answer = (TextMessage)consumer2.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    // https://issues.apache.org/activemq/browse/AMQ-2567
    public void testManyMessageConsumer() throws Exception {
        doTestManyMessageConsumer(true);
    }

    public void testManyMessageConsumerNoTransaction() throws Exception {
        doTestManyMessageConsumer(false);
    }

    private void doTestManyMessageConsumer(boolean transacted) throws Exception {
        Session session = connection.createSession(transacted, transacted ? Session.SESSION_TRANSACTED : Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));
        producer.send(session.createTextMessage("Msg2"));
        producer.send(session.createTextMessage("Msg3"));
        producer.send(session.createTextMessage("Msg4"));
        producer.send(session.createTextMessage("Msg5"));
        producer.send(session.createTextMessage("Msg6"));
        producer.send(session.createTextMessage("Msg7"));
        producer.send(session.createTextMessage("Msg8"));
        if (transacted) {
            session.commit();
        }
        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);

        MessageConsumer consumer2  = session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg1");
        if (transacted) {
            session.commit();
        }
        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg2");
        if (transacted) {
            session.commit();
        }
        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg3");
        if (transacted) {
            session.commit();
        }
        // this call would return null if prefetchSize > 0
        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg4");
        if (transacted) {
            session.commit();
        }
        // Now using other consumer
        // this call should return the next message (Msg5) still left on the queue
        answer = (TextMessage)consumer2.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg5");
        if (transacted) {
            session.commit();
        }
        // Now using other consumer
        // this call should return the next message still left on the queue
        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg6");
        // read one more message without commit
        // this call should return the next message still left on the queue
        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg7");
        if (transacted) {
            session.commit();
        }
        // Now using other consumer
        // this call should return the next message (Msg5) still left on the queue
        answer = (TextMessage)consumer2.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg8");
        if (transacted) {
            session.commit();
        }
        answer = (TextMessage)consumer.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    public void testManyMessageConsumerWithSend() throws Exception {
        doTestManyMessageConsumerWithSend(true);
    }

    public void testManyMessageConsumerWithTxSendPrioritySupport() throws Exception {
        ((ActiveMQConnection)connection).setMessagePrioritySupported(true);
        doTestManyMessageConsumerWithSend(true);
    }

    public void testManyMessageConsumerWithSendNoTransaction() throws Exception {
        doTestManyMessageConsumerWithSend(false);
    }

    private void doTestManyMessageConsumerWithSend(boolean transacted) throws Exception {
        Session session = connection.createSession(transacted, transacted ? Session.SESSION_TRANSACTED :Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(queue);
        producer.send(session.createTextMessage("Msg1"));
        producer.send(session.createTextMessage("Msg2"));
        producer.send(session.createTextMessage("Msg3"));
        producer.send(session.createTextMessage("Msg4"));
        producer.send(session.createTextMessage("Msg5"));
        producer.send(session.createTextMessage("Msg6"));
        producer.send(session.createTextMessage("Msg7"));
        producer.send(session.createTextMessage("Msg8"));
        if (transacted) {
            session.commit();
        }
        // now lets receive it
        MessageConsumer consumer = session.createConsumer(queue);

        MessageConsumer consumer2  = session.createConsumer(queue);
        TextMessage answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg1");
        if (transacted) {
            session.commit();
        }
        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg2");
        if (transacted) {
            session.commit();
        }
        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg3");
        if (transacted) {
            session.commit();
        }
        // Now using other consumer take 2
        answer = (TextMessage)consumer2.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg4");
        answer = (TextMessage)consumer2.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg5");

        // ensure prefetch extension ok by sending another that could get dispatched
        producer.send(session.createTextMessage("Msg9"));
        if (transacted) {
            session.commit();
        }

        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg6");
        // read one more message without commit
        // and using other consumer
        answer = (TextMessage)consumer2.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg7");
        if (transacted) {
            session.commit();
        }

        answer = (TextMessage)consumer2.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg8");
        if (transacted) {
            session.commit();
        }

        answer = (TextMessage)consumer.receive(5000);
        assertEquals("Should have received a message!", answer.getText(), "Msg9");
        if (transacted) {
            session.commit();
        }
        answer = (TextMessage)consumer.receiveNoWait();
        assertNull("Should have not received a message!", answer);
    }

    // https://issues.apache.org/jira/browse/AMQ-4224
    public void testBrokerZeroPrefetchConfig() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = session.createProducer(brokerZeroQueue);
        producer.send(session.createTextMessage("Msg1"));
        // now lets receive it
        MessageConsumer consumer = session.createConsumer(brokerZeroQueue);

        TextMessage answer = (TextMessage)consumer.receive(5000);
        assertNotNull("Consumer should have read a message", answer);
        assertEquals("Should have received a message!", answer.getText(), "Msg1");
    }

    // https://issues.apache.org/jira/browse/AMQ-4234
    // https://issues.apache.org/jira/browse/AMQ-4235
    public void testBrokerZeroPrefetchConfigWithConsumerControl() throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        ActiveMQMessageConsumer consumer = (ActiveMQMessageConsumer) session.createConsumer(brokerZeroQueue);
        assertEquals("broker config prefetch in effect", 0, consumer.info.getCurrentPrefetchSize());

        // verify sub view broker
        Subscription sub =
                broker.getRegionBroker().getDestinationMap().get(ActiveMQDestination.transform(brokerZeroQueue)).getConsumers().get(0);
        assertEquals("broker sub prefetch is correct", 0, sub.getConsumerInfo().getCurrentPrefetchSize());

        // manipulate Prefetch (like failover and stomp)
        ConsumerControl consumerControl = new ConsumerControl();
        consumerControl.setConsumerId(consumer.info.getConsumerId());
        consumerControl.setDestination(ActiveMQDestination.transform(brokerZeroQueue));
        consumerControl.setPrefetch(1000); // default for a q

        Object reply = ((ActiveMQConnection) connection).getTransport().request(consumerControl);
        assertTrue("good request", !(reply instanceof ExceptionResponse));
        assertEquals("broker config prefetch in effect", 0, consumer.info.getCurrentPrefetchSize());
        assertEquals("broker sub prefetch is correct", 0, sub.getConsumerInfo().getCurrentPrefetchSize());
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService brokerService = super.createBroker();
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry zeroPrefetchPolicy = new PolicyEntry();
        zeroPrefetchPolicy.setQueuePrefetch(0);
        policyMap.put(ActiveMQDestination.transform(brokerZeroQueue), zeroPrefetchPolicy);
        brokerService.setDestinationPolicy(policyMap);
        return brokerService;
    }

    @Override
    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:0";
        super.setUp();

        connection = createConnection();
        connection.start();
        queue = createQueue();
    }

    @Override
    protected void startBroker() throws Exception {
        super.startBroker();
        bindAddress = broker.getTransportConnectors().get(0).getConnectUri().toString();
    }

    @Override
    protected void tearDown() throws Exception {
        try {
            connection.close();
        } catch (Exception ex) {}

        super.tearDown();
    }

    protected Queue createQueue() {
        return new ActiveMQQueue(getDestinationString() + "?consumer.prefetchSize=0");
    }
}
