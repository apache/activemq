/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.bugs;

import java.lang.reflect.Field;
import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ProducerId;
import org.apache.activemq.transport.vm.VMTransportFactory;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author <a href="http://www.christianposta.com/blog">Christian Posta</a>
 */
public class AMQ4222Test extends TestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ4222Test.class);

    protected BrokerService brokerService;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        topic = false;
        brokerService = createBroker();
    }

    @Override
    protected void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = BrokerFactory.createBroker(new URI("broker:()/localhost?persistent=false"));
        broker.start();
        broker.waitUntilStarted();
        return broker;
    }

    @Override
    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://localhost");
    }

    public void testTempQueueCleanedUp() throws Exception {

        Destination requestQueue = createDestination();

        Connection producerConnection = createConnection();
        producerConnection.start();
        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);

        MessageProducer producer = producerSession.createProducer(requestQueue);
        Destination replyTo = producerSession.createTemporaryQueue();
        MessageConsumer producerSessionConsumer = producerSession.createConsumer(replyTo);

        final CountDownLatch countDownLatch = new CountDownLatch(1);
        // let's listen to the response on the queue
        producerSessionConsumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    if (message instanceof TextMessage) {
                        LOG.info("You got a message: " + ((TextMessage) message).getText());
                        countDownLatch.countDown();
                    }
                } catch (JMSException e) {
                    e.printStackTrace();
                }
            }
        });

        producer.send(createRequest(producerSession, replyTo));

        Connection consumerConnection = createConnection();
        consumerConnection.start();
        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(requestQueue);
        final MessageProducer consumerProducer = consumerSession.createProducer(null);

        consumer.setMessageListener(new MessageListener() {
            @Override
            public void onMessage(Message message) {
                try {
                    consumerProducer.send(message.getJMSReplyTo(), message);
                } catch (JMSException e) {
                    LOG.error("error sending a response on the temp queue");
                    e.printStackTrace();
                }
            }
        });

        countDownLatch.await(2, TimeUnit.SECONDS);

        // producer has not gone away yet...
        org.apache.activemq.broker.region.Destination tempDestination = getDestination(brokerService,
                (ActiveMQDestination) replyTo);
        assertNotNull(tempDestination);

        // clean up
        producer.close();
        producerSession.close();
        producerConnection.close();

        // producer has gone away.. so the temp queue should not exist anymore... let's see..
        // producer has not gone away yet...
        tempDestination = getDestination(brokerService,
                (ActiveMQDestination) replyTo);
        assertNull(tempDestination);

        // now.. the connection on the broker side for the dude producing to the temp dest will
        // still have a reference in his producerBrokerExchange.. this will keep the destination
        // from being reclaimed by GC if there is never another send that producer makes...
        // let's see if that reference is there...
        final TransportConnector connector = VMTransportFactory.CONNECTORS.get("localhost");
        assertNotNull(connector);
        assertTrue(Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return connector.getConnections().size() == 1;
            }
        }));
        TransportConnection transportConnection = connector.getConnections().get(0);
        Map<ProducerId, ProducerBrokerExchange> exchanges = getProducerExchangeFromConn(transportConnection);
        assertEquals(1, exchanges.size());
        ProducerBrokerExchange exchange = exchanges.values().iterator().next();

        // so this is the reason for the test... we don't want these exchanges to hold a reference
        // to a region destination.. after a send is completed, the destination is not used anymore on
        // a producer exchange
        assertNull(exchange.getRegionDestination());
        assertNull(exchange.getRegion());

    }

    @SuppressWarnings("unchecked")
    private Map<ProducerId, ProducerBrokerExchange> getProducerExchangeFromConn(TransportConnection transportConnection) throws NoSuchFieldException, IllegalAccessException {
        Field f = TransportConnection.class.getDeclaredField("producerExchanges");
        f.setAccessible(true);
        Map<ProducerId, ProducerBrokerExchange> producerExchanges =
                (Map<ProducerId, ProducerBrokerExchange>)f.get(transportConnection);
        return producerExchanges;
    }

    private Message createRequest(Session session, Destination replyTo) throws JMSException {
        Message message = session.createTextMessage("Payload");
        message.setJMSReplyTo(replyTo);
        return message;
    }
}
