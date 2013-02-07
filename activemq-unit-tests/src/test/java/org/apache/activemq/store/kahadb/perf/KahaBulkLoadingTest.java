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
package org.apache.activemq.store.kahadb.perf;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.BytesMessage;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.Test;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This tests bulk loading and unloading of messages to a Queue.s
 *
 *
 */
public class KahaBulkLoadingTest extends JmsTestSupport {

    private static final Logger LOG = LoggerFactory.getLogger(KahaBulkLoadingTest.class);

    protected int messageSize = 1024 * 4;

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService broker = new BrokerService();
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(new File("target/activemq-data/kahadb"));
        // kaha.deleteAllMessages();
        broker.setPersistenceAdapter(kaha);
        broker.addConnector("tcp://localhost:0");
        return broker;
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws URISyntaxException, IOException {
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getServer().getConnectURI());
        factory.setUseAsyncSend(true);
        return factory;
    }

    public void testQueueSendThenAddConsumer() throws Exception {
        long start;
        long end;
        ActiveMQDestination destination = new ActiveMQQueue("TEST");

        connection.setUseCompression(false);
        connection.getPrefetchPolicy().setAll(10);
        connection.start();

        Session session = connection.createSession(false, Session.DUPS_OK_ACKNOWLEDGE);

        LOG.info("Receiving messages that are in the queue");
        MessageConsumer consumer = session.createConsumer(destination);
        BytesMessage msg = (BytesMessage)consumer.receive(2000);
        int consumed = 0;
        if( msg!=null ) {
            consumed++;
        }
        while (true) {
            int counter = 0;
            if (msg == null) {
                break;
            }
            end = start = System.currentTimeMillis();
            int size = 0;
            while ((end - start) < 5000) {
                msg = (BytesMessage)consumer.receive(5000);
                if (msg == null) {
                    break;
                }
                counter++;
                consumed++;
                end = System.currentTimeMillis();
                size += msg.getBodyLength();
            }
            LOG.info("Consumed: " + (counter * 1000.0 / (end - start)) + " " + " messages/sec, " + (1.0 * size / (1024.0 * 1024.0)) * ((1000.0 / (end - start))) + " megs/sec ");
        }
        consumer.close();
        LOG.info("Consumed " + consumed + " messages from the queue.");

        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.PERSISTENT);

        LOG.info("Sending messages that are " + (messageSize / 1024.0) + "k large");
        // Send a message to the broker.
        start = System.currentTimeMillis();

        final AtomicBoolean stop = new AtomicBoolean();
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                stop.set(true);
            }
        });

        int produced = 0;
        while (!stop.get()) {
            end = start = System.currentTimeMillis();
            int produceCount = 0;
            while ((end - start) < 5000 && !stop.get()) {
                BytesMessage bm = session.createBytesMessage();
                bm.writeBytes(new byte[messageSize]);
                producer.send(bm);
                produceCount++;
                produced++;
                end = System.currentTimeMillis();
            }
            LOG.info("Produced: " + (produceCount * 1000.0 / (end - start)) + " messages/sec, " + (1.0 * produceCount * messageSize / (1024.0 * 1024.0)) * ((1000.0 / (end - start))) + " megs/sec");
        }
        LOG.info("Prodcued " + produced + " messages to the queue.");

    }

    public static Test suite() {
        return suite(KahaBulkLoadingTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

}
