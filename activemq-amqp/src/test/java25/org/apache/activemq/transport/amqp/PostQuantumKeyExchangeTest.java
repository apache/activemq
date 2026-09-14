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
package org.apache.activemq.transport.amqp;

import static org.apache.activemq.transport.amqp.SslTestSupport.KEYSTORE;
import static org.apache.activemq.transport.amqp.SslTestSupport.addConnector;
import static org.apache.activemq.transport.amqp.SslTestSupport.handshake;
import static org.apache.activemq.transport.amqp.SslTestSupport.sslContext;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * Lives in src/test/java27, so it only runs on JDK 27 and later, where TLS 1.3
 * offers the hybrid post-quantum key exchange groups. Every SSL capable
 * transport is bound with the new options and probed with raw handshakes whose
 * client side offers chosen groups.
 */
@Category(ParallelTest.class)
@RunWith(Parameterized.class)
public class PostQuantumKeyExchangeTest {

    private static final String[] CLASSICAL_ONLY = {"x25519", "secp256r1"};
    private static final String[] HYBRID_ONLY = {"X25519MLKEM768"};

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> transports() {
        return Arrays.asList(new Object[][] {
            {"ssl"},
            {"nio+ssl"},
            {"auto+ssl"},
            {"auto+nio+ssl"},
            {"amqp+ssl"},
            {"amqp+nio+ssl"},
            {"mqtt+ssl"},
            {"mqtt+nio+ssl"},
            {"stomp+ssl"},
            {"stomp+nio+ssl"},
        });
    }

    @Parameterized.Parameter
    public String transport;

    private BrokerService broker;

    @Before
    public void setUp() {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(false);
        broker.setAdvisorySupport(false);
    }

    @After
    public void tearDown() throws Exception {
        broker.stop();
        broker.waitUntilStopped();
    }

    @Test(timeout = 60000)
    public void requiredPostQuantumKeyExchangeRefusesClassicalOnlyClients() throws Exception {
        TransportConnector connector = start("?transport.requirePostQuantumKeyExchange=true");

        assertEquals("TLSv1.3", handshake(connector, null));
        assertEquals("TLSv1.3", handshake(connector, HYBRID_ONLY));
        assertThrows(IOException.class, () -> handshake(connector, CLASSICAL_ONLY));
    }

    @Test(timeout = 60000)
    public void namedGroupsSelectTheHybridGroup() throws Exception {
        TransportConnector connector = start("?transport.namedGroups=SecP256r1MLKEM768");

        assertEquals("TLSv1.3", handshake(connector, new String[] {"SecP256r1MLKEM768"}));
        assertThrows(IOException.class, () -> handshake(connector, HYBRID_ONLY));
        assertThrows(IOException.class, () -> handshake(connector, CLASSICAL_ONLY));
    }

    @Test(timeout = 60000)
    public void defaultConnectorStillAcceptsClassicalClients() throws Exception {
        TransportConnector connector = start("");

        assertEquals("TLSv1.3", handshake(connector, CLASSICAL_ONLY));
        assertEquals("TLSv1.3", handshake(connector, HYBRID_ONLY));
    }

    @Test(timeout = 60000)
    public void requireAndNamedGroupsTogetherStopTheConnector() throws Exception {
        addConnector(broker, transport + "://localhost:0?transport.requirePostQuantumKeyExchange=true&transport.namedGroups=x25519", sslContext(KEYSTORE));
        Exception thrown = assertThrows(Exception.class, () -> broker.start());
        String messages = "";
        for (Throwable t = thrown; t != null; t = t.getCause()) {
            messages += t.getMessage() + " | ";
        }
        assertTrue(messages, messages.contains("cannot both be set"));
    }

    private TransportConnector start(String options) throws Exception {
        TransportConnector connector = addConnector(broker, transport + "://localhost:0" + options, sslContext(KEYSTORE));
        broker.start();
        broker.waitUntilStarted();
        return connector;
    }
}
