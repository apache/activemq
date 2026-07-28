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
package org.apache.activemq.transport;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.eclipse.jetty.server.ConnectionFactory;
import org.eclipse.jetty.server.HttpConfiguration;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.SecureRequestCustomizer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.junit.Test;

/**
 * Verifies that the TLS SNI host-check flag can be configured per transportConnector via a
 * {@code transport.sniHostCheck} option (bubbled onto {@link SecureSocketConnectorFactory}), and
 * that the configured value is honored - previously the flag was always forced to {@code false}
 * whenever the legacy jetty.ssl.sniHostCheck system property was merely present.
 */
public class SecureSocketConnectorFactorySniTest {

    private SecureRequestCustomizer sniCustomizerFor(SecureSocketConnectorFactory factory) throws Exception {
        Server server = new Server();
        // createConnector builds the connector without starting it, so no keystore is loaded.
        ServerConnector connector = (ServerConnector) factory.createConnector(server);
        for (ConnectionFactory cf : connector.getConnectionFactories()) {
            if (cf instanceof HttpConnectionFactory) {
                HttpConfiguration config = ((HttpConnectionFactory) cf).getHttpConfiguration();
                for (HttpConfiguration.Customizer c : config.getCustomizers()) {
                    if (c instanceof SecureRequestCustomizer) {
                        return (SecureRequestCustomizer) c;
                    }
                }
            }
        }
        return null;
    }

    private SecureSocketConnectorFactory factoryWithOption(String value) {
        SecureSocketConnectorFactory factory = new SecureSocketConnectorFactory();
        Map<String, Object> options = new HashMap<>();
        // transport.sniHostCheck=<value> arrives here with the "transport." prefix already stripped.
        options.put("sniHostCheck", value);
        factory.setTransportOptions(options);
        return factory;
    }

    @Test
    public void testSniHostCheckTransportOptionFalseIsHonored() throws Exception {
        SecureRequestCustomizer customizer = sniCustomizerFor(factoryWithOption("false"));
        assertNotNull("A SecureRequestCustomizer should be installed when sniHostCheck is set", customizer);
        assertFalse("transport.sniHostCheck=false must disable the SNI host check", customizer.isSniHostCheck());
    }

    @Test
    public void testSniHostCheckTransportOptionTrueIsHonored() throws Exception {
        SecureRequestCustomizer customizer = sniCustomizerFor(factoryWithOption("true"));
        assertNotNull("A SecureRequestCustomizer should be installed when sniHostCheck is set", customizer);
        assertTrue("transport.sniHostCheck=true must enforce the SNI host check (was previously forced false)",
                customizer.isSniHostCheck());
    }
}
