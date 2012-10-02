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

import static org.junit.Assert.assertTrue;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Ensures connections aren't leaked when when we use backup=true and randomize=false
 */
public class FailoverBackupLeakTest {

    private static BrokerService s1, s2;

    @BeforeClass
    public static void setUp() throws Exception {
        s1 = buildBroker("broker1");
        s2 = buildBroker("broker2");

        s1.start();
        s1.waitUntilStarted();
        s2.start();
        s2.waitUntilStarted();
    }

    @AfterClass
    public static void tearDown() throws Exception {
        if (s2 != null) {
            s2.stop();
            s2.waitUntilStopped();
        }
        if (s1 != null) {
            s1.stop();
            s1.waitUntilStopped();
        }
    }

    private static String getConnectString(BrokerService service) throws Exception {
        return service.getTransportConnectors().get(0).getPublishableConnectString();
    }

    private static BrokerService buildBroker(String brokerName) throws Exception {
        BrokerService service = new BrokerService();
        service.setBrokerName(brokerName);
        service.setUseJmx(false);
        service.setPersistent(false);
        service.setUseShutdownHook(false);
        service.addConnector("tcp://0.0.0.0:0?transport.closeAsync=false");
        return service;
    }

    @Test
    public void backupNoRandomize() throws Exception {
        check("backup=true&randomize=false");
    }

    @Test
    public void priorityBackupNoRandomize() throws Exception {
        check("priorityBackup=true&randomize=false");
    }

    private void check(String connectionProperties) throws Exception {
        String s1URL = getConnectString(s1), s2URL = getConnectString(s2);
        String uri = "failover://(" + s1URL + "," + s2URL + ")?" + connectionProperties;
        ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(uri);
        for (int i = 0; i < 10; i++) {
            buildConnection(factory);
        }

        assertTrue(connectionProperties +  " broker1 connection count not zero: was["+getConnectionCount(s1)+"]", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getConnectionCount(s1) == 0;
            }
        }));

        assertTrue(connectionProperties +  " broker2 connection count not zero: was["+getConnectionCount(s2)+"]", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return getConnectionCount(s2) == 0;
            }
        }));
    }

    private int getConnectionCount(BrokerService service) {
        return service.getTransportConnectors().get(0).getConnections().size();
    }

    private void buildConnection(ConnectionFactory local) throws JMSException {
        Connection conn = null;
        Session sess = null;
        try {
            conn = local.createConnection();
            sess =  conn.createSession(false, Session.CLIENT_ACKNOWLEDGE);
        } finally {
            try { if (sess != null) sess.close(); } catch (JMSException ignore) { }
            try { if (conn != null) conn.close(); } catch (JMSException ignore) { }
        }
    }
}
