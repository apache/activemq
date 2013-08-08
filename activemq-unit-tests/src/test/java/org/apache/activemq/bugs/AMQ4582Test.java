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

import java.io.IOException;

import javax.jms.Connection;
import javax.jms.Session;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.ConsumerThread;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AMQ4582Test {

    private static final transient Logger LOG = LoggerFactory.getLogger(AMQ4582Test.class);

    BrokerService broker;
    Connection connection;
    Session session;

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    public static final int PRODUCER_COUNT = 10;
    public static final int CONSUMER_COUNT = 10;
    public static final int MESSAGE_COUNT = 1000;

    final ConsumerThread[] consumers = new ConsumerThread[CONSUMER_COUNT];

    @Before
    public void setUp() throws Exception {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
    }

    @After
    public void tearDown() throws Exception {
        if (broker != null) {
            try {
                broker.stop();
            } catch(Exception e) {}
        }
    }

    @Rule public ExpectedException thrown = ExpectedException.none();
    @Test
    public void simpleTest() throws Exception {
        thrown.expect(IOException.class);
        thrown.expectMessage("enabledCipherSuites=BADSUITE");

        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        try {
            broker.addConnector(
                "ssl://localhost:0?transport.needClientAuth=true&transport.enabledCipherSuites=BADSUITE");
            broker.start();
            broker.waitUntilStarted();
        } catch (Exception e) {
            LOG.info("BrokerService threw:", e);
            throw e;
        }
    }
}
