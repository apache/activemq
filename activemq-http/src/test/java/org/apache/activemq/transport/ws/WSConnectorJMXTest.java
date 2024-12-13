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
package org.apache.activemq.transport.ws;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.jmx.ConnectorViewMBean;
import org.apache.activemq.util.Wait;
import org.junit.Before;
import org.junit.Test;

import javax.management.ObjectName;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class WSConnectorJMXTest {

    private BrokerService broker;

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setDeleteAllMessagesOnStartup(true);
        broker.setPersistent(true);
        broker.setUseJmx(true);
        broker.addConnector("ws://localhost:0").setName("Default");
        broker.setBrokerName("localhost");
        broker.start();
        broker.waitUntilStarted(30_000);
        broker.deleteAllMessages();
    }

    @Test
    public void testRestartConnection() throws Exception {
        Thread.currentThread().setContextClassLoader(new ClassLoader(null) {});

        ConnectorViewMBean connectionView = getProxyToConnectionView();
        connectionView.stop();
        assertTrue("Connection should close", Wait.waitFor(() -> !connectionView.isStarted(),
                TimeUnit.SECONDS.toMillis(60), 500));
        connectionView.start();
        assertTrue("Connection should open", Wait.waitFor(connectionView::isStarted,
                TimeUnit.SECONDS.toMillis(60), 500));
    }

    protected ConnectorViewMBean getProxyToConnectionView() throws Exception {
        ObjectName connectorQuery = new ObjectName(
                "org.apache.activemq:type=Broker,brokerName=localhost,connector=clientConnectors,connectorName=Default");

        return (ConnectorViewMBean) broker.getManagementContext()
                .newProxyInstance(connectorQuery, ConnectorViewMBean.class, true);
    }
}
