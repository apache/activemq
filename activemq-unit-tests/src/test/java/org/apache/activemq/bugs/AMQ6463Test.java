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

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.JmsTestSupport;
import org.apache.activemq.ScheduledMessage;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.broker.region.policy.PolicyEntry;
import org.apache.activemq.broker.region.policy.PolicyMap;
import org.apache.activemq.broker.region.policy.VMPendingQueueMessageStoragePolicy;
import org.apache.activemq.broker.region.policy.VMPendingSubscriberMessageStoragePolicy;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.IOHelper;
import org.apache.activemq.util.Wait;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.ConnectionFactory;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.TextMessage;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class AMQ6463Test extends JmsTestSupport {
    static final Logger LOG = LoggerFactory.getLogger(AMQ6463Test.class);
    ActiveMQQueue queueA = new ActiveMQQueue("QUEUE.A");
    protected TransportConnector connector;
    protected ActiveMQConnection connection;
    protected DefaultTestAppender appender;
    final AtomicInteger errors = new AtomicInteger(0);
    final AtomicBoolean gotUsageBlocked = new AtomicBoolean();


    public void testBlockedSechedulerSendNoError() throws Exception {
        ActiveMQConnectionFactory factory = (ActiveMQConnectionFactory)createConnectionFactory();
        connection = (ActiveMQConnection)factory.createConnection();
        connections.add(connection);
        connection.start();

        final Session session = connection.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        final MessageProducer producer = session.createProducer(queueA);

        TextMessage message = session.createTextMessage("test msg");
        final int numMessages = 20;

        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_DELAY, 0);
        message.setLongProperty(ScheduledMessage.AMQ_SCHEDULED_PERIOD, 0);
        message.setIntProperty(ScheduledMessage.AMQ_SCHEDULED_REPEAT, numMessages - 1);

        producer.send(message);
        producer.close();

        // lets not consume till producer is blocked
        assertTrue("got blocked event", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return gotUsageBlocked.get();
            }
        }, 60000));

        MessageConsumer consumer = session.createConsumer(queueA);
        TextMessage msg;
        for (int idx = 0; idx < numMessages; ++idx) {
            msg = (TextMessage) consumer.receive(10000);
            assertNotNull("received: " + idx, msg);
            msg.acknowledge();
        }
        assertTrue("no errors in the log", errors.get() == 0);
        assertTrue("got blocked message", gotUsageBlocked.get());
    }


    protected BrokerService createBroker() throws Exception {
        BrokerService service = new BrokerService();
        service.setPersistent(true);
        service.setUseJmx(false);
        service.setSchedulerSupport(true);
        service.setDeleteAllMessagesOnStartup(true);

        IOHelper.deleteChildren(service.getSchedulerDirectoryFile());

        service.getSystemUsage().getMemoryUsage().setLimit(512);

        // Setup a destination policy where it takes only 1 message at a time.
        PolicyMap policyMap = new PolicyMap();
        PolicyEntry policy = new PolicyEntry();
        policy.setExpireMessagesPeriod(0); // so not to effect memory usage
        policy.setMemoryLimit(1);
        policy.setPendingSubscriberPolicy(new VMPendingSubscriberMessageStoragePolicy());
        policy.setPendingQueuePolicy(new VMPendingQueueMessageStoragePolicy());
        policy.setProducerFlowControl(true);
        policyMap.setDefaultEntry(policy);
        service.setDestinationPolicy(policyMap);

        connector = service.addConnector("tcp://localhost:0");
        return service;
    }

    public void setUp() throws Exception {
        setAutoFail(true);
        appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.ERROR)) {
                    errors.incrementAndGet();
                } else if (event.getLevel().equals(Level.WARN) && event.getRenderedMessage().contains("Usage Manager Memory Limit")) {
                    gotUsageBlocked.set(true);
                }
            }
        };
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        rootLogger.addAppender(appender);

        super.setUp();
    }

    protected void tearDown() throws Exception {
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        rootLogger.removeAppender(appender);

        if (connection != null) {
            connection.close();
        }
        super.tearDown();
    }

    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(connector.getConnectUri());
    }
}
