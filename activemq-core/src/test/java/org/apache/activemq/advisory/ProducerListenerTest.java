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
import javax.jms.JMSException;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 
 * @version $Revision: 359679 $
 */
public class ProducerListenerTest extends EmbeddedBrokerTestSupport implements ProducerListener {
    private static final Log LOG = LogFactory.getLog(ProducerListenerTest.class);

    protected Session consumerSession1;
    protected Session consumerSession2;
    protected int consumerCounter;
    protected ProducerEventSource producerEventSource;
    protected BlockingQueue eventQueue = new ArrayBlockingQueue(1000);
    private Connection connection;

    public void testProducerEvents() throws Exception {
        producerEventSource.start();

        consumerSession1 = createProducer();
        assertConsumerEvent(1, true);

        consumerSession2 = createProducer();
        assertConsumerEvent(2, true);

        consumerSession1.close();
        consumerSession1 = null;
        assertConsumerEvent(1, false);

        consumerSession2.close();
        consumerSession2 = null;
        assertConsumerEvent(0, false);
    }

    public void testListenWhileAlreadyConsumersActive() throws Exception {
        consumerSession1 = createProducer();
        consumerSession2 = createProducer();

        producerEventSource.start();
        assertConsumerEvent(2, true);
        assertConsumerEvent(2, true);

        consumerSession1.close();
        consumerSession1 = null;
        assertConsumerEvent(1, false);

        consumerSession2.close();
        consumerSession2 = null;
        assertConsumerEvent(0, false);
    }

    public void onProducerEvent(ProducerEvent event) {
        eventQueue.add(event);
    }

    protected void setUp() throws Exception {
        super.setUp();

        connection = createConnection();
        connection.start();
        producerEventSource = new ProducerEventSource(connection, destination);
        producerEventSource.setProducerListener(this);
    }

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

    protected void assertConsumerEvent(int count, boolean started) throws InterruptedException {
        ProducerEvent event = waitForProducerEvent();
        assertEquals("Producer count", count, event.getProducerCount());
        assertEquals("started", started, event.isStarted());
    }

    protected Session createProducer() throws JMSException {
        final String consumerText = "Consumer: " + (++consumerCounter);
        LOG.info("Creating consumer: " + consumerText + " on destination: " + destination);

        Session answer = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = answer.createProducer(destination);
        return answer;
    }

    protected ProducerEvent waitForProducerEvent() throws InterruptedException {
        ProducerEvent answer = (ProducerEvent)eventQueue.poll(100000, TimeUnit.MILLISECONDS);
        assertTrue("Should have received a consumer event!", answer != null);
        return answer;
    }

}
