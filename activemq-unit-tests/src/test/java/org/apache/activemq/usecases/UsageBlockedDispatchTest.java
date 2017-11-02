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
package org.apache.activemq.usecases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQPrefetchPolicy;
import org.apache.activemq.TestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.usage.SystemUsage;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.log4j.Appender;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;

import javax.jms.*;
import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

public class UsageBlockedDispatchTest extends TestSupport {

    private static final int MESSAGES_COUNT = 100;
    private static byte[] buf = new byte[2 * 1024];
    private BrokerService broker;

    protected long messageReceiveTimeout = 4000L;

    private String connectionUri;

    @Override
    public void setUp() throws Exception {

        broker = new BrokerService();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);

        setDefaultPersistenceAdapter(broker);
        SystemUsage sysUsage = broker.getSystemUsage();
        sysUsage.getMemoryUsage().setLimit(100*1024);

        PolicyEntry defaultPolicy = new PolicyEntry();
        defaultPolicy.setProducerFlowControl(false);
        defaultPolicy.setCursorMemoryHighWaterMark(100);
        defaultPolicy.setMemoryLimit(50*1024);

        PolicyMap policyMap = new PolicyMap();
        policyMap.setDefaultEntry(defaultPolicy);
        broker.setDestinationPolicy(policyMap);
        broker.setSystemUsage(sysUsage);

        broker.addConnector("tcp://localhost:0").setName("Default");
        broker.start();

        connectionUri = broker.getTransportConnectors().get(0).getPublishableConnectString();
    }

    @Override
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
        }
    }

    public void testFillMemToBlockConsumer() throws Exception {

        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(connectionUri);
        ActiveMQPrefetchPolicy prefetch = new ActiveMQPrefetchPolicy();
        prefetch.setTopicPrefetch(10);
        factory.setPrefetchPolicy(prefetch);

        final Connection producerConnection = factory.createConnection();
        producerConnection.start();

        Session producerSession = producerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageProducer producer = producerSession.createProducer(null);
        BytesMessage message = producerSession.createBytesMessage();
        message.writeBytes(buf);

        int numFillers = 4;
        ArrayList<ActiveMQQueue> fillers = new ArrayList<ActiveMQQueue>();
        for (int i=0; i<numFillers; i++) {
            fillers.add(new ActiveMQQueue("Q" + i));
        }

        // fill cache and consume all memory
        for (int idx = 0; idx < MESSAGES_COUNT; ++idx) {
            for (ActiveMQQueue q : fillers) {
                producer.send(q, message);
            }
        }
        ActiveMQQueue willGetAPage = new ActiveMQQueue("Q" + numFillers++);
        for (int idx = 0; idx < MESSAGES_COUNT; ++idx) {
            producer.send(willGetAPage, message);
        }

        ActiveMQQueue shouldBeStuckForDispatch = new ActiveMQQueue("Q" + numFillers);
        for (int idx = 0; idx < MESSAGES_COUNT; ++idx) {
            producer.send(shouldBeStuckForDispatch, message);
        }

        Connection consumerConnection = factory.createConnection();
        consumerConnection.start();

        Session consumerSession = consumerConnection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        MessageConsumer consumer = consumerSession.createConsumer(willGetAPage);

        Message m = consumer.receive(messageReceiveTimeout);
        assertNotNull("got a message", m);

        final AtomicBoolean gotExpectedLogEvent = new AtomicBoolean(false);
        Appender appender = new DefaultTestAppender() {

            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel() == Level.WARN && event.getRenderedMessage().contains("cursor blocked")) {
                    gotExpectedLogEvent.set(true);
                }
            }
        };

        try {
            org.apache.log4j.Logger.getLogger(Queue.class).addAppender(appender);

            MessageConsumer noDispatchConsumer = consumerSession.createConsumer(shouldBeStuckForDispatch);

            m = noDispatchConsumer.receive(messageReceiveTimeout);
            assertNull("did not get a message", m);

            assertTrue("Got the new warning about the blocked cursor", gotExpectedLogEvent.get());
        } finally {
            org.apache.log4j.Logger.getLogger(Queue.class).removeAppender(appender);
            org.apache.log4j.Logger.getRootLogger().removeAppender(appender);
        }
    }
}
