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
package org.apache.activemq.transport.failover;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import jakarta.jms.Connection;

import static org.junit.Assert.assertTrue;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.xbean.BrokerFactoryBean;
import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.ClassPathResource;

/**
 * Tests for AMQ-3719
 */
public class ConnectionHangOnStartupTest {

    private static final Logger LOG = LoggerFactory.getLogger(ConnectionHangOnStartupTest.class);

    protected String uriString;
    protected BrokerService master = null;
    protected AtomicReference<BrokerService> slave = new AtomicReference<BrokerService>();

    @After
    public void tearDown() throws Exception {

        BrokerService brokerService = slave.get();
        if (brokerService != null) {
            brokerService.stop();
        }
        if (master != null)
            master.stop();
    }

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory(uriString);
    }

    protected void createMaster() throws Exception {
        // Use ephemeral port for XML-based broker config
        System.setProperty("masterPort", "0");

        BrokerFactoryBean brokerFactory = new BrokerFactoryBean(new ClassPathResource(getMasterXml()));
        brokerFactory.afterPropertiesSet();
        master = brokerFactory.getBroker();
        master.start();

        // Get actual port and build failover URI with short maxInactivityDurationInitalDelay
        // to trigger the bug, and short maxReconnectDelay so the test runs faster
        final int masterPort = master.getTransportConnectors().get(0).getConnectUri().getPort();
        System.setProperty("slavePort", String.valueOf(masterPort));
        uriString = "failover://(tcp://localhost:" + masterPort
                + "?wireFormat.maxInactivityDurationInitalDelay=1)?randomize=false&maxReconnectDelay=200";
    }

    protected void createSlave() throws Exception {
        BrokerFactoryBean brokerFactory = new BrokerFactoryBean(new ClassPathResource(getSlaveXml()));
        brokerFactory.afterPropertiesSet();
        BrokerService broker = brokerFactory.getBroker();
        broker.start();
        slave.set(broker);
    }

    protected String getSlaveXml() {
        return "org/apache/activemq/broker/ft/sharedFileSlave.xml";
    }

    protected String getMasterXml() {
        return "org/apache/activemq/broker/ft/sharedFileMaster.xml";
    }

    @Test(timeout=60000)
    public void testInitialWireFormatNegotiationTimeout() throws Exception {
        final AtomicReference<Connection> conn = new AtomicReference<>();
        final CountDownLatch connStarted = new CountDownLatch(1);

        // Must create master first to get the ephemeral port and build uriString
        createMaster();

        final Thread t = new Thread(() -> {
            try {
                conn.set(createConnectionFactory().createConnection());
                conn.get().start();
            } catch (Exception ex) {
                LOG.error("could not create or start connection", ex);
            }
            connStarted.countDown();
        });
        t.start();

        // Wait for connection to be established
        assertTrue("connection started", connStarted.await(30, TimeUnit.SECONDS));
        conn.get().stop();
    }

}
