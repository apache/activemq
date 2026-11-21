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

import static org.junit.Assert.assertEquals;

import java.util.HashMap;

import javax.security.auth.Subject;

import org.apache.activemq.util.ActiveMQEncryptor;
import org.junit.Test;

public class EncryptableLDAPLoginModuleTest {

    @Test
    public void testEncryptionPasswordIsUsedAsWritten() {
        var options = new HashMap<String, Object>();
        options.put("encryptionPassword", "ENC(master)");
        options.put("connectionURL", "ldap://localhost:1024");
        options.put("connectionPassword", "secret");

        new EncryptableLDAPLoginModule().initialize(new Subject(), null, new HashMap<>(), options);
    }

    @Test
    public void testOnlyLdapOptionsAreDecrypted() {
        var encryptor = new ActiveMQEncryptor();
        encryptor.setPassword("ENC(master)");

        var options = new HashMap<String, Object>();
        options.put("encryptionPassword", "ENC(master)");
        options.put("passwordEnvName", "ENC(NAME)");
        options.put("encryptionAlgorithm", "ENC(ALGORITHM)");
        options.put("connectionURL", "ldap://localhost:1024");
        options.put("connectionPassword", ActiveMQEncryptor.wrapEncryptedValue(encryptor.encrypt("secret")));

        var decrypted = EncryptableLDAPLoginModule.decryptOptions(options, encryptor);

        assertEquals("ENC(master)", decrypted.get("encryptionPassword"));
        assertEquals("ENC(NAME)", decrypted.get("passwordEnvName"));
        assertEquals("ENC(ALGORITHM)", decrypted.get("encryptionAlgorithm"));
        assertEquals("ldap://localhost:1024", decrypted.get("connectionURL"));
        assertEquals("secret", decrypted.get("connectionPassword"));
    }
}
