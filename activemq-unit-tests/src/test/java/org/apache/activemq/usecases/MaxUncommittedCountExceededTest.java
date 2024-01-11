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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.Collection;

import javax.management.JMX;
import javax.management.MBeanServer;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.BrokerMBeanSupport;
import org.apache.activemq.broker.jmx.QueueViewMBean;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageProducer;
import jakarta.jms.Queue;
import jakarta.jms.ResourceAllocationException;
import jakarta.jms.Session;

@RunWith(value = Parameterized.class)
public class MaxUncommittedCountExceededTest {

    public static final String DEFAULT_JMX_DOMAIN_NAME = "org.apache.activemq";
    public static final String DEFAULT_JMX_BROKER_NAME = "localhost";

    public static final String DEFAULT_JMS_USER = "admin";
    public static final String DEFAULT_JMS_PASS = "admin";

    @Parameterized.Parameters(name="syncSend={0}, exceptionContains={1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(new Object[][] {
                {true, "Can not send message on transaction with id: "},
                {false, "has not been started."}
        });
    }

    private final boolean syncSend;
    private final String exceptionContains;

    public MaxUncommittedCountExceededTest(boolean syncSend, String exceptionContains) {
        this.syncSend = syncSend;
        this.exceptionContains = exceptionContains;
    }

    protected ActiveMQConnectionFactory activemqConnectionFactory = null;
    protected BrokerService brokerService = null;

    @Rule
    public TestName testName = new TestName();

    // Control session
    protected Connection connection = null;
    protected Session session = null;
    protected MessageProducer messageProducer = null;

    protected String methodNameDestinationName = null;
    protected MBeanServer mbeanServer = null;
    protected QueueViewMBean queueViewMBean = null;

    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();
        brokerService.setDeleteAllMessagesOnStartup(true);
        brokerService.setPersistent(true);
        brokerService.setUseJmx(true);
        brokerService.addConnector("tcp://localhost:0").setName("Default");
        brokerService.setBrokerName("localhost");
        brokerService.start();
        brokerService.waitUntilStarted(30_000);
        brokerService.deleteAllMessages();
        assertNotNull(brokerService);

        activemqConnectionFactory = new ActiveMQConnectionFactory(brokerService.getTransportConnectorByScheme("tcp").getPublishableConnectString());
        connection = activemqConnectionFactory.createConnection();
        connection.start();
        session = connection.createSession(true, Session.SESSION_TRANSACTED);
        methodNameDestinationName = "AMQ.TX." + cleanParameterizedMethodName(testName.getMethodName().toUpperCase());
        Queue queue = session.createQueue(methodNameDestinationName);
        messageProducer = session.createProducer(queue);
        mbeanServer = ManagementFactory.getPlatformMBeanServer();
        brokerService.getAdminView().addQueue(methodNameDestinationName);
        queueViewMBean = getQueueViewMBean(new ActiveMQQueue(methodNameDestinationName));
    }

    @After
    public void tearDown() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
            } finally {
                connection = null;
            }
        }

        methodNameDestinationName = null;
        activemqConnectionFactory = null;
        if(brokerService != null) {
            brokerService.deleteAllMessages();
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    protected static String cleanParameterizedMethodName(String methodName) {
        // clean up parameterized method string:
        // TESTMESSAGETIMESTAMPTIMETOLIVE[DESTINATIONTYPE=QUEUE, MESSAGETYPE=BYTES]
        // returns: TESTMESSAGETIMESTAMPTIMETOLIVE.QUEUE.BYTES

        if (methodName == null || (!methodName.contains("[") && !methodName.contains("]"))) {
            return methodName;
        }

        String[] step1 = methodName.split("\\[", 2);
        String[] step2 = step1[1].split("\\]", 2);
        String[] step3 = step2[0].split(",", 16);

        return step1[0] + "." + step3[0].split("=", 2)[1] + "." + step3[1].split("=", 2)[1];
    }

    protected QueueViewMBean getQueueViewMBean(ActiveMQDestination destination) throws Exception {
        return JMX.newMBeanProxy(mbeanServer, BrokerMBeanSupport.createDestinationName(BrokerMBeanSupport.createBrokerObjectName(DEFAULT_JMX_DOMAIN_NAME, DEFAULT_JMX_BROKER_NAME).toString(), destination), QueueViewMBean.class);
    }

    protected void configureConnection(Connection connection, boolean syncSend) {
        if(syncSend) {
            ActiveMQConnection activemqConnection = (ActiveMQConnection)connection;
            activemqConnection.setAlwaysSyncSend(true);
            activemqConnection.setUseAsyncSend(false);
            activemqConnection.setProducerWindowSize(10);
        }
    }

    @Test
    public void testUncommittedCountExceeded() throws Exception {
        assertEquals(Long.valueOf(0l), Long.valueOf(brokerService.getAdminView().getTotalMaxUncommittedExceededCount()));
        assertEquals(Long.valueOf(0l), Long.valueOf(queueViewMBean.getMaxUncommittedExceededCount()));

        brokerService.setMaxUncommittedCount(10);
        boolean caught = false;
        JMSException caughtException = null;

        configureConnection(connection, syncSend);

        try {
            for(int i=0; i < 20; i++) {
                Message message = session.createBytesMessage();
                message.setIntProperty("IDX", i);
                messageProducer.send(message);
            }

            if(!syncSend) {
                session.commit();
            }
        } catch (JMSException e) {
            if(syncSend) {
                assertTrue(e instanceof ResourceAllocationException);
            }
            caught = true;
            caughtException = e;
        }

        assertTrue(caught);
        assertNotNull(caughtException);
        assertTrue(caughtException.getMessage().contains(exceptionContains));
        assertEquals(Long.valueOf(1l), Long.valueOf(brokerService.getAdminView().getTotalMaxUncommittedExceededCount()));
        assertEquals(Long.valueOf(1l), Long.valueOf(queueViewMBean.getMaxUncommittedExceededCount()));
    }
}
