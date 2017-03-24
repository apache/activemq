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
package org.apache.activemq.broker.jmx;

import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HealthViewMBeanTest extends EmbeddedBrokerTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(MBeanTest.class);
    protected MBeanServer mbeanServer;
    protected String domain = "org.apache.activemq";

    @Override
    protected void setUp() throws Exception {
        bindAddress = "tcp://localhost:0";
        useTopic = false;
        super.setUp();
        mbeanServer = broker.getManagementContext().getMBeanServer();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    @Override
    protected ConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(broker.getTransportConnectors().get(0).getPublishableConnectString());
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        BrokerService answer = new BrokerService();
        answer.setPersistent(true);
        answer.setDeleteAllMessagesOnStartup(true);
        answer.getSystemUsage().getMemoryUsage().setLimit(1024 * 1024 * 64);
        answer.getSystemUsage().getTempUsage().setLimit(1024 * 1024 * 64);
        answer.getSystemUsage().getStoreUsage().setLimit(1024 * 1024 * 64);
        answer.getSystemUsage().getJobSchedulerUsage().setLimit(1024 * 1024 * 64);
        answer.setUseJmx(true);
        answer.setSchedulerSupport(true);

        // allow options to be visible via jmx

        answer.addConnector(bindAddress);
        return answer;
    }

    public void testHealthView() throws Exception{
        Connection connection = connectionFactory.createConnection();

        connection.start();
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        destination = createDestination();
        MessageProducer producer = session.createProducer(destination);
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);

        for (int i = 0; i < 60; i++) {
            BytesMessage message = session.createBytesMessage();
            message.writeBytes(new byte[1024 *1024]);
            producer.send(message);
        }

        Thread.sleep(1000);

        String objectNameStr = broker.getBrokerObjectName().toString();
        objectNameStr += ",service=Health";
        ObjectName brokerName = assertRegisteredObjectName(objectNameStr);
        HealthViewMBean health =  MBeanServerInvocationHandler.newProxyInstance(mbeanServer, brokerName, HealthViewMBean.class, true);
        List<HealthStatus> list = health.healthList();

        for (HealthStatus status : list) {
            LOG.info("Health status: {}", status);
        }

        assertEquals(2, list.size());

        String healthStatus = health.healthStatus();
        String currentStatus = health.getCurrentStatus();

        assertEquals(healthStatus, currentStatus);
    }

    protected ObjectName assertRegisteredObjectName(String name) throws MalformedObjectNameException, NullPointerException {
        ObjectName objectName = new ObjectName(name);
        if (mbeanServer.isRegistered(objectName)) {
            LOG.info("Bean Registered: " + objectName);
        } else {
            fail("Could not find MBean!: " + objectName);
        }
        return objectName;
    }
}
