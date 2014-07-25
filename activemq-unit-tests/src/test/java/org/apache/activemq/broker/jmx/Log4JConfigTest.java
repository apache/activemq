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

import javax.jms.ConnectionFactory;
import javax.management.MBeanServer;
import javax.management.MBeanServerInvocationHandler;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.EmbeddedBrokerTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.junit.Test;
import org.slf4j.LoggerFactory;

public class Log4JConfigTest extends EmbeddedBrokerTestSupport {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(Log4JConfigTest.class);

    private static final String BROKER_LOGGER = "org.apache.activemq.broker.BrokerService";

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
        answer.setUseJmx(true);
        answer.setSchedulerSupport(true);
        answer.addConnector(bindAddress);
        return answer;
    }

    @Test
    public void testLog4JConfigViewExists() throws Exception {
        String brokerObjectName = broker.getBrokerObjectName().toString();
        String log4jConfigViewName = BrokerMBeanSupport.createLog4JConfigViewName(brokerObjectName).toString();
        assertRegisteredObjectName(log4jConfigViewName);
    }

    @Test
    public void testLog4JConfigViewGetLoggers() throws Throwable {
        String brokerObjectName = broker.getBrokerObjectName().toString();
        ObjectName log4jConfigViewName = BrokerMBeanSupport.createLog4JConfigViewName(brokerObjectName);
        Log4JConfigViewMBean log4jConfigView =  MBeanServerInvocationHandler.newProxyInstance(
            mbeanServer, log4jConfigViewName, Log4JConfigViewMBean.class, true);

        List<String> loggers = log4jConfigView.getLoggers();
        assertNotNull(loggers);
        assertFalse(loggers.isEmpty());
    }

    @Test
    public void testLog4JConfigViewGetLevel() throws Throwable {
        String brokerObjectName = broker.getBrokerObjectName().toString();
        ObjectName log4jConfigViewName = BrokerMBeanSupport.createLog4JConfigViewName(brokerObjectName);
        Log4JConfigViewMBean log4jConfigView =  MBeanServerInvocationHandler.newProxyInstance(
            mbeanServer, log4jConfigViewName, Log4JConfigViewMBean.class, true);

        String level = log4jConfigView.getLogLevel(BROKER_LOGGER);
        assertNotNull(level);
        assertFalse(level.isEmpty());
    }

    @Test
    public void testLog4JConfigViewGetLevelUnknownLoggerName() throws Throwable {
        String brokerObjectName = broker.getBrokerObjectName().toString();
        ObjectName log4jConfigViewName = BrokerMBeanSupport.createLog4JConfigViewName(brokerObjectName);
        Log4JConfigViewMBean log4jConfigView =  MBeanServerInvocationHandler.newProxyInstance(
            mbeanServer, log4jConfigViewName, Log4JConfigViewMBean.class, true);

        // Non-existent loggers will return a name equal to the root level.
        String level = log4jConfigView.getLogLevel("not.a.logger");
        assertNotNull(level);
        assertFalse(level.isEmpty());
        assertEquals(Logger.getRootLogger().getLevel().toString(), level);
    }

    @Test
    public void testLog4JConfigViewSetLevel() throws Throwable {
        String brokerObjectName = broker.getBrokerObjectName().toString();
        ObjectName log4jConfigViewName = BrokerMBeanSupport.createLog4JConfigViewName(brokerObjectName);
        Log4JConfigViewMBean log4jConfigView =  MBeanServerInvocationHandler.newProxyInstance(
            mbeanServer, log4jConfigViewName, Log4JConfigViewMBean.class, true);

        String level = log4jConfigView.getLogLevel(BROKER_LOGGER);
        assertNotNull(level);
        assertFalse(level.isEmpty());

        log4jConfigView.setLogLevel(BROKER_LOGGER, "WARN");
        level = log4jConfigView.getLogLevel(BROKER_LOGGER);
        assertNotNull(level);
        assertEquals("WARN", level);

        log4jConfigView.setLogLevel(BROKER_LOGGER, "INFO");
        level = log4jConfigView.getLogLevel(BROKER_LOGGER);
        assertNotNull(level);
        assertEquals("INFO", level);
    }

    @Test
    public void testLog4JConfigViewSetLevelNoChangeIfLevelIsBad() throws Throwable {
        String brokerObjectName = broker.getBrokerObjectName().toString();
        ObjectName log4jConfigViewName = BrokerMBeanSupport.createLog4JConfigViewName(brokerObjectName);
        Log4JConfigViewMBean log4jConfigView =  MBeanServerInvocationHandler.newProxyInstance(
            mbeanServer, log4jConfigViewName, Log4JConfigViewMBean.class, true);

        log4jConfigView.setLogLevel(BROKER_LOGGER, "INFO");
        String level = log4jConfigView.getLogLevel(BROKER_LOGGER);
        assertNotNull(level);
        assertEquals("INFO", level);

        log4jConfigView.setLogLevel(BROKER_LOGGER, "BAD");
        level = log4jConfigView.getLogLevel(BROKER_LOGGER);
        assertNotNull(level);
        assertEquals("INFO", level);
    }

    @Test
    public void testLog4JConfigViewGetRootLogLevel() throws Throwable {
        String brokerObjectName = broker.getBrokerObjectName().toString();
        ObjectName log4jConfigViewName = BrokerMBeanSupport.createLog4JConfigViewName(brokerObjectName);
        Log4JConfigViewMBean log4jConfigView =  MBeanServerInvocationHandler.newProxyInstance(
            mbeanServer, log4jConfigViewName, Log4JConfigViewMBean.class, true);

        String level = log4jConfigView.getRootLogLevel();
        assertNotNull(level);
        assertFalse(level.isEmpty());

        String currentRootLevel = Logger.getRootLogger().getLevel().toString();
        assertEquals(currentRootLevel, level);
    }

    @Test
    public void testLog4JConfigViewSetRootLevel() throws Throwable {
        String brokerObjectName = broker.getBrokerObjectName().toString();
        ObjectName log4jConfigViewName = BrokerMBeanSupport.createLog4JConfigViewName(brokerObjectName);
        Log4JConfigViewMBean log4jConfigView =  MBeanServerInvocationHandler.newProxyInstance(
            mbeanServer, log4jConfigViewName, Log4JConfigViewMBean.class, true);

        String currentRootLevel = Logger.getRootLogger().getLevel().toString();
        log4jConfigView.setRootLogLevel("WARN");
        currentRootLevel = Logger.getRootLogger().getLevel().toString();
        assertEquals("WARN", currentRootLevel);
        log4jConfigView.setRootLogLevel("INFO");
        currentRootLevel = Logger.getRootLogger().getLevel().toString();
        assertEquals("INFO", currentRootLevel);

        Level level;
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
