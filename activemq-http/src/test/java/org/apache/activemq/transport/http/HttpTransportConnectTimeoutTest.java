/*
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
package org.apache.activemq.transport.http;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ExceptionListener;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HttpTransportConnectTimeoutTest {

    private static final Logger LOG = LoggerFactory.getLogger(HttpTransportConnectTimeoutTest.class);

    private BrokerService broker;
    private ActiveMQConnectionFactory factory;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        TransportConnector connector = broker.addConnector(
            "http://localhost:0?trace=true&transport.connectAttemptTimeout=2000");
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.start();

        String connectionUri = connector.getPublishableConnectString();
        factory = new ActiveMQConnectionFactory(connectionUri + "?trace=true");
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
    }

    @Test(timeout = 60000)
    public void testSendReceiveAfterPause() throws Exception {
        final CountDownLatch failed = new CountDownLatch(1);

        Connection connection = factory.createConnection();
        connection.start();
        connection.setExceptionListener(new ExceptionListener() {

            @Override
            public void onException(JMSException exception) {
                LOG.info("Connection failed due to: {}", exception.getMessage());
                failed.countDown();
            }
        });

        assertFalse(failed.await(3, TimeUnit.SECONDS));

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createTemporaryQueue();
        MessageProducer producer = session.createProducer(queue);
        MessageConsumer consumer = session.createConsumer(queue);

        producer.send(session.createMessage());

        assertNotNull(consumer.receive(5000));
    }
}
