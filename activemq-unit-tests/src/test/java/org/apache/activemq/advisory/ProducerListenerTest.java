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
package org.apache.activemq.advisory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 *
 */
public class ProducerListenerTest extends EmbeddedBrokerTestSupport implements ProducerListener {
    private static final Logger LOG = LoggerFactory.getLogger(ProducerListenerTest.class);

    protected Session consumerSession1;
    protected Session consumerSession2;
    protected int consumerCounter;
    protected ProducerEventSource producerEventSource;
    protected BlockingQueue<ProducerEvent> eventQueue = new ArrayBlockingQueue<ProducerEvent>(1000);
    private Connection connection;

    public void testProducerEvents() throws Exception {
        producerEventSource.start();

        consumerSession1 = createProducer();
        assertProducerEvent(1, true);

        consumerSession2 = createProducer();
        assertProducerEvent(2, true);

        consumerSession1.close();
        consumerSession1 = null;
        assertProducerEvent(1, false);

        consumerSession2.close();
        consumerSession2 = null;
        assertProducerEvent(0, false);
    }

    public void testListenWhileAlreadyConsumersActive() throws Exception {
        consumerSession1 = createProducer();
        consumerSession2 = createProducer();

        producerEventSource.start();
        assertProducerEvent(2, true);
        assertProducerEvent(2, true);

        consumerSession1.close();
        consumerSession1 = null;
        assertProducerEvent(1, false);

        consumerSession2.close();
        consumerSession2 = null;
        assertProducerEvent(0, false);
    }

    public void testConsumerEventsOnTemporaryDestination() throws Exception {

        Session s = connection.createSession(true,Session.AUTO_ACKNOWLEDGE);
        Destination dest = useTopic ? s.createTemporaryTopic() : s.createTemporaryQueue();
        producerEventSource = new ProducerEventSource(connection, dest);
        producerEventSource.setProducerListener(this);
        producerEventSource.start();
        MessageProducer producer = s.createProducer(dest);
        assertProducerEvent(1, true);
        producer.close();
        assertProducerEvent(0, false);
    }



    @Override
    public void onProducerEvent(ProducerEvent event) {
        eventQueue.add(event);
    }

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        connection = createConnection();
        connection.start();
        producerEventSource = new ProducerEventSource(connection, destination);
        producerEventSource.setProducerListener(this);
    }

    @Override
    protected void tearDown() throws Exception {
        if (producerEventSource != null) {
            producerEventSource.stop();
        }
        if (consumerSession2 != null) {
            consumerSession2.close();
        }
        if (consumerSession1 != null) {
            consumerSession1.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

    protected void assertProducerEvent(int count, boolean started) throws InterruptedException {
        ProducerEvent event = waitForProducerEvent();
        assertEquals("Producer count", count, event.getProducerCount());
        assertEquals("started", started, event.isStarted());
    }

    protected Session createProducer() throws JMSException {
        final String consumerText = "Consumer: " + (++consumerCounter);
        LOG.info("Creating consumer: " + consumerText + " on destination: " + destination);

        Session answer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = answer.createProducer(destination);
        assertNotNull(producer);

        return answer;
    }

    protected ProducerEvent waitForProducerEvent() throws InterruptedException {
        ProducerEvent answer = eventQueue.poll(100000, TimeUnit.MILLISECONDS);
        assertTrue("Should have received a consumer event!", answer != null);
        return answer;
    }

}
