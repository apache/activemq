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
import org.apache.activemq.util.Wait;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;
import jakarta.jms.*;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.experimental.categories.Category;

@Category(ParallelTest.class)
public class UsageBlockedDispatchTest extends TestSupport {

    private static final int MESSAGES_COUNT = 100;
    private static byte[] buf = new byte[2 * 1024];
    private BrokerService broker;

    protected long messageReceiveTimeout = 4000L;

    private String connectionUri;

    @Override
    public void setUp() throws Exception {

        broker = createBroker();
        broker.setDataDirectory("target" + File.separator + "activemq-data");
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.setAdvisorySupport(false);
        broker.setDeleteAllMessagesOnStartup(true);

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

    protected BrokerService createBroker() throws IOException {
        BrokerService broker = new BrokerService();
        setDefaultPersistenceAdapter(broker);
        return broker;
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

        consumer.receive(messageReceiveTimeout);

        final AtomicBoolean gotExpectedLogEvent = new AtomicBoolean(false);
        // start new
        final var logger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getLogger(Queue.class));
        final var appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                if (Level.WARN.equals(event.getLevel()) && event.getMessage().getFormattedMessage().contains("cursor blocked")) {
                    gotExpectedLogEvent.set(true);
                }
            }
        };
        appender.start();

        logger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        logger.addAppender(appender);

        try {

            assertTrue("Timed out waiting for cursor to block", Wait.waitFor(() -> gotExpectedLogEvent.get()));

            MessageConsumer noDispatchConsumer = consumerSession.createConsumer(shouldBeStuckForDispatch);

            Message m = noDispatchConsumer.receive(messageReceiveTimeout);
            assertNull("did not get a message", m);

            assertTrue("Got the new warning about the blocked cursor", gotExpectedLogEvent.get());
        } finally {
            logger.removeAppender(appender);
        }
    }
}
