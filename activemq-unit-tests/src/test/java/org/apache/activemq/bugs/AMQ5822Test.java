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

import javax.jms.BytesMessage;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ5822Test {

    private static final Logger LOG = LoggerFactory.getLogger(AMQ5822Test.class);

    private BrokerService brokerService;
    private String connectionUri;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.addConnector("nio://0.0.0.0:0?"
            + "wireFormat.maxInactivityDurationInitalDelay=1000&wireFormat.maxInactivityDuration=300");
        brokerService.start();

        connectionUri = brokerService.getTransportConnectorByScheme("nio").getPublishableConnectString();
    }

    @After
    public void tearDown() throws Exception {
        brokerService.stop();
        brokerService.waitUntilStopped();
    }

    @Test
    public void testReadCounter() throws Exception {
        LOG.info("Connecting to: {}", connectionUri);

        byte[] payload = new byte[50 * 1024 * 1024];

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri + "?useInactivityMonitor=false");
        final ActiveMQConnection connection = (ActiveMQConnection) factory.createConnection();
        connection.start();

        LOG.info("Connected to: {}", connection.getTransport());

        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Queue queue = session.createQueue("test");
        MessageProducer producer = session.createProducer(queue);
        BytesMessage message = session.createBytesMessage();

        message.writeBytes(payload);

        producer.setDeliveryMode(DeliveryMode.PERSISTENT);
        producer.send(message);

        connection.close();
    }
}
