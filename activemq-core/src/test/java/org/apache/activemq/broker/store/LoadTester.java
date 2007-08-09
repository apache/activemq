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
package org.apache.activemq.broker.store;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.ProgressPrinter;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.memory.list.SimpleMessageList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * @version $Revision$
 */
public class LoadTester extends JmsTestSupport {

    private static final Log LOG = LogFactory.getLog(LoadTester.class);
    
    protected int messageSize = 1024 * 64;
    protected int produceCount = 10000;

    protected BrokerService createBroker() throws Exception {
        return BrokerFactory.createBroker(new URI("xbean:org/apache/activemq/broker/store/loadtester.xml"));
    }

    protected ConnectionFactory createConnectionFactory() throws URISyntaxException, IOException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(((TransportConnector)broker.getTransportConnectors().get(0)).getServer().getConnectURI());
        factory.setUseAsyncSend(true);
        return factory;
    }

    public void testQueueSendThenAddConsumer() throws Exception {
        ProgressPrinter printer = new ProgressPrinter(produceCount, 20);

        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        connection.setUseCompression(false);
        connection.getPrefetchPolicy().setAll(10);
        connection.start();
        Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        LOG.info("Sending " + produceCount + " messages that are " + (messageSize / 1024.0) + "k large, for a total of " + (produceCount * messageSize / (1024.0 * 1024.0))
                 + " megs of data.");
        // Send a message to the broker.
        long start = System.currentTimeMillis();
        for (int i = 0; i < produceCount; i++) {
            printer.increment();
            BytesMessage msg = session.createBytesMessage();
            msg.writeBytes(new byte[messageSize]);
            producer.send(msg);
        }
        long end1 = System.currentTimeMillis();

        LOG.info("Produced messages/sec: " + (produceCount * 1000.0 / (end1 - start)));

        printer = new ProgressPrinter(produceCount, 10);
        start = System.currentTimeMillis();
        MessageConsumer consumer = session.createConsumer(destination);
        for (int i = 0; i < produceCount; i++) {
            printer.increment();
            assertNotNull("Getting message: " + i, consumer.receive(20000));
        }
        end1 = System.currentTimeMillis();
        LOG.info("Consumed messages/sec: " + (produceCount * 1000.0 / (end1 - start)));

    }

    public static Test suite() {
        return suite(LoadTester.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
