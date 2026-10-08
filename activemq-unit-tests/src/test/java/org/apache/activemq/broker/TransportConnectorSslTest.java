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
package org.apache.activemq.broker;

import static org.junit.Assert.assertEquals;

import java.net.URI;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.activemq.broker.jmx.ConnectorView;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Connector.isSsl(): the bound transport server's answer once started, the
 * configured scheme before that.
 */
@Category(ParallelTest.class)
public class TransportConnectorSslTest {

    private static final String KEYSTORE = "src/test/resources/org/apache/activemq/security/broker1.ks";

    private BrokerService broker;

    @BeforeClass
    public static void keystore() {
        System.setProperty("javax.net.ssl.keyStore", KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", "password");
        System.setProperty("javax.net.ssl.keyStoreType", "jks");
        System.setProperty("javax.net.ssl.trustStore", KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", "password");
        System.setProperty("javax.net.ssl.trustStoreType", "jks");
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            broker.stop();
            broker.waitUntilStopped();
        }
    }

    @Test
    public void testConfiguredSchemeDecidesBeforeStart() throws Exception {
        var expectations = new LinkedHashMap<String, Boolean>();
        expectations.put("tcp", false);
        expectations.put("nio", false);
        expectations.put("auto", false);
        expectations.put("auto+nio", false);
        expectations.put("vm", false);
        expectations.put("http", false);
        expectations.put("ws", false);
        expectations.put("ssl", true);
        expectations.put("nio+ssl", true);
        expectations.put("auto+ssl", true);
        expectations.put("auto+nio+ssl", true);
        expectations.put("mqtt+ssl", true);
        expectations.put("stomp+nio+ssl", true);
        expectations.put("amqp+nio+ssl", true);
        expectations.put("https", true);
        expectations.put("wss", true);
        expectations.put("SSL", true);

        for (Map.Entry<String, Boolean> expectation : expectations.entrySet()) {
            var connector = new TransportConnector();
            connector.setUri(new URI(expectation.getKey() + "://localhost:0"));
            assertEquals(expectation.getKey(), expectation.getValue(), connector.isSsl());
        }
        assertEquals("no uri configured", false, new TransportConnector().isSsl());
    }

    @Test(timeout = 60000)
    public void testBoundServerDecidesAfterStart() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        var expectations = new LinkedHashMap<TransportConnector, Boolean>();
        expectations.put(broker.addConnector("tcp://localhost:0"), false);
        expectations.put(broker.addConnector("nio://localhost:0"), false);
        expectations.put(broker.addConnector("auto://localhost:0"), false);
        expectations.put(broker.addConnector("ssl://localhost:0"), true);
        expectations.put(broker.addConnector("nio+ssl://localhost:0"), true);
        expectations.put(broker.addConnector("auto+ssl://localhost:0"), true);
        expectations.put(broker.addConnector("auto+nio+ssl://localhost:0"), true);
        expectations.put(broker.addConnector("mqtt+ssl://localhost:0"), true);
        expectations.put(broker.addConnector("stomp+nio+ssl://localhost:0"), true);
        broker.start();
        broker.waitUntilStarted();

        for (Map.Entry<TransportConnector, Boolean> expectation : expectations.entrySet()) {
            var connector = expectation.getKey();
            var label = connector.getUri().toString();
            assertEquals(label, expectation.getValue(), connector.isSsl());
            assertEquals(label + " via JMX view", expectation.getValue(), new ConnectorView(connector).isSsl());
        }
    }
}
