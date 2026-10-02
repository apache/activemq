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
package org.apache.activemq.jaas;

import java.security.cert.X509Certificate;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;

import junit.framework.TestCase;

/**
 * The handler must hand every connection attribute it was given to the login
 * module's ConnectionCallback, and tolerate having none.
 */
public class JassCredentialCallbackHandlerTest extends TestCase {

    public void testCopiesEveryConnectionAttribute() throws Exception {
        var certificates = new X509Certificate[0];
        var given = new ConnectionCallback();
        given.setConnectionId("ID:host-1-1:1");
        given.setClientId("order-1");
        given.setBrokerName("broker1");
        given.setNetworkConnection(true);
        given.setSsl(true);
        given.setRemoteAddress("tcp://127.0.0.1:5000");
        given.setTransportConnectorName("openwire");
        given.setCertificates(certificates);

        var name = new NameCallback("Username: ");
        var password = new PasswordCallback("Password: ", false);
        var asked = new ConnectionCallback();
        new JassCredentialCallbackHandler("order", "secret", given).handle(new Callback[] {name, password, asked});

        assertEquals("order", name.getName());
        assertEquals("secret", new String(password.getPassword()));
        assertEquals("ID:host-1-1:1", asked.getConnectionId());
        assertEquals("order-1", asked.getClientId());
        assertEquals("broker1", asked.getBrokerName());
        assertTrue(asked.isNetworkConnection());
        assertTrue(asked.isSsl());
        assertEquals("tcp://127.0.0.1:5000", asked.getRemoteAddress());
        assertEquals("openwire", asked.getTransportConnectorName());
        assertSame(certificates, asked.getCertificates());
    }

    public void testNoConnectionLeavesCallbackUntouched() throws Exception {
        var asked = new ConnectionCallback();
        new JassCredentialCallbackHandler("order", "secret").handle(new Callback[] {asked});

        assertNull(asked.getConnectionId());
        assertNull(asked.getClientId());
        assertFalse(asked.isNetworkConnection());
        assertFalse(asked.isSsl());
        assertNull(asked.getCertificates());
    }
}
