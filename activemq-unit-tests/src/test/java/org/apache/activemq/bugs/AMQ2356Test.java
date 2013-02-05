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

import java.io.File;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.store.kahadb.KahaDBStore;

/*
 A AMQ2356Test
 We have an environment where we have a very large number of destinations.
 In an effort to reduce the number of threads I have set the options
 -Dorg.apache.activemq.UseDedicatedTaskRunner=false

 and

 <policyEntry queue=">" optimizedDispatch="true"/>

 Unfortunately this very quickly leads to deadlocked queues.

 My environment is:

 ActiveMQ 5.2 Ubunty Jaunty kernel 2.6.28-14-generic #47-Ubuntu SMP (although only a single core on my system)
 TCP transportConnector

 To reproduce the bug (which I can do 100% of the time) I connect 5 consumers (AUTO_ACK) to 5 different queues.
 Then I start 5 producers and pair them up with a consumer on a queue, and they start sending PERSISTENT messages.
 I've set the producer to send 100 messages and disconnect, and the consumer to receive 100 messages and disconnect.
 The first pair usually gets through their 100 messages and disconnect, at which point all the other pairs have
 deadlocked at less than 30 messages each.
 */
public class AMQ2356Test extends TestCase {
    protected static final int MESSAGE_COUNT = 1000;
    protected static final int NUMBER_OF_PAIRS = 10;
    protected BrokerService broker;
    protected String brokerURL = ActiveMQConnectionFactory.DEFAULT_BROKER_BIND_URL;
    protected int destinationCount;

    public void testScenario() throws Exception {
        for (int i = 0; i < NUMBER_OF_PAIRS; i++) {
            ActiveMQQueue queue = new ActiveMQQueue(getClass().getName() + ":" + i);
            ProducerConsumerPair cp = new ProducerConsumerPair();
            cp.start(this.brokerURL, queue, MESSAGE_COUNT);
            cp.testRun();
            cp.stop();
        }
    }

    protected Destination getDestination(Session session) throws JMSException {
        String destinationName = getClass().getName() + "." + destinationCount++;
        return session.createQueue(destinationName);
    }

    @Override
    protected void setUp() throws Exception {
        if (broker == null) {
            broker = createBroker();
        }
        super.setUp();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (broker != null) {
            broker.stop();
        }
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        configureBroker(answer);
        answer.start();
        return answer;
    }

    protected void configureBroker(BrokerService answer) throws Exception {
        File dataFileDir = new File("target/test-amq-data/bugs/AMQ2356/kahadb");
        KahaDBStore kaha = new KahaDBStore();
        kaha.setDirectory(dataFileDir);
        answer.setUseJmx(false);
        // Setup a destination policy where it takes only 1 message at a time.
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setOptimizedDispatch(true);
        policyMap.setDefaultEntry(policy);
        answer.setDestinationPolicy(policyMap);

        answer.setAdvisorySupport(false);
        answer.setEnableStatistics(false);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.addConnector(brokerURL);

    }

    static class ProducerConsumerPair {
        private Destination destination;
        private MessageProducer producer;
        private MessageConsumer consumer;
        private Connection producerConnection;
        private Connection consumerConnection;
        private int numberOfMessages;

        ProducerConsumerPair() {

        }

        void start(String brokerURL, final Destination dest, int msgNum) throws Exception {
            this.destination = dest;
            this.numberOfMessages = msgNum;
            ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(brokerURL);
            this.producerConnection = cf.createConnection();
            this.producerConnection.start();
            this.consumerConnection = cf.createConnection();
            this.consumerConnection.start();
            this.producer = createProducer(this.producerConnection);
            this.consumer = createConsumer(this.consumerConnection);
        }

        void testRun() throws Exception {

            Session s = this.producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            for (int i = 0; i < this.numberOfMessages; i++) {
                BytesMessage msg = s.createBytesMessage();
                msg.writeBytes(new byte[1024]);
                this.producer.send(msg);
            }
            int received = 0;
            for (int i = 0; i < this.numberOfMessages; i++) {
                Message msg = this.consumer.receive();
                assertNotNull(msg);
                received++;
            }
            assertEquals("Messages received on " + this.destination, this.numberOfMessages, received);

        }

        void stop() throws Exception {
            if (this.producerConnection != null) {
                this.producerConnection.close();
            }
            if (this.consumerConnection != null) {
                this.consumerConnection.close();
            }
        }

        private MessageProducer createProducer(Connection connection) throws Exception {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageProducer result = session.createProducer(this.destination);
            return result;
        }

        private MessageConsumer createConsumer(Connection connection) throws Exception {

            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            MessageConsumer result = session.createConsumer(this.destination);
            return result;
        }
    }
}
