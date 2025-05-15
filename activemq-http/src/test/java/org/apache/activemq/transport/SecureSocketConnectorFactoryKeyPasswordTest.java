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

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.junit.Test;

/**
 * Verifies that a key (key-manager) password distinct from the keystore password is applied to the
 * {@link SslContextFactory} built for a secure transportConnector. The keystore fixture has store
 * password "storepwd" and key password "keypwd"; recovering the private key requires the key
 * password. The positive test is a regression guard: previously the factory never called
 * setKeyManagerPassword, so key recovery failed even when a key password was configured.
 */
public class SecureSocketConnectorFactoryKeyPasswordTest {

    private static final String KEYSTORE = "src/test/resources/keystore-diff-key-password.jks";
    private static final String STORE_PASSWORD = "storepwd";
    private static final String KEY_PASSWORD = "keypwd";

    private SslContextFactory.Server buildSslContextFactory(SecureSocketConnectorFactory factory) throws Exception {
        Server server = new Server();
        ServerConnector connector = (ServerConnector) factory.createConnector(server);
        SslConnectionFactory sslConnectionFactory = connector.getConnectionFactory(SslConnectionFactory.class);
        return sslConnectionFactory.getSslContextFactory();
    }

    @Test
    public void testDistinctKeyPasswordAllowsKeyRecovery() throws Exception {
        SecureSocketConnectorFactory factory = new SecureSocketConnectorFactory();
        factory.setKeyStore(KEYSTORE);
        factory.setKeyStoreType("JKS");
        factory.setKeyStorePassword(STORE_PASSWORD);
        factory.setKeyPassword(KEY_PASSWORD); // key password differs from the store password

        SslContextFactory.Server scf = buildSslContextFactory(factory);
        try {
            // start() loads the keystore and recovers the private key using the key-manager password.
            scf.start();
            assertTrue("SslContextFactory should start once the distinct key password is applied",
                    scf.isStarted());
        } finally {
            scf.stop();
        }
    }

    @Test
    public void testMissingKeyPasswordFailsWhenKeyPasswordDiffers() throws Exception {
        SecureSocketConnectorFactory factory = new SecureSocketConnectorFactory();
        factory.setKeyStore(KEYSTORE);
        factory.setKeyStoreType("JKS");
        factory.setKeyStorePassword(STORE_PASSWORD);
        factory.setKeyPassword(null); // fall back to the store password, which cannot recover the key

        SslContextFactory.Server scf = buildSslContextFactory(factory);
        try {
            scf.start();
            fail("Expected key recovery to fail when the distinct key password is not provided");
        } catch (Exception expected) {
            // UnrecoverableKeyException (wrapped): the key password is required but was not supplied.
        } finally {
            try {
                scf.stop();
            } catch (Exception ignored) {
                // best-effort cleanup after a failed start
            }
        }
    }
}
