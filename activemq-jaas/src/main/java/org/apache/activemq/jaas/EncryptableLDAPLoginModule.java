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

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;

import org.apache.activemq.util.ActiveMQEncryptor;

/**
 * LDAPLoginModule that supports encryption
 */
public class EncryptableLDAPLoginModule extends LDAPLoginModule {

    private static final String ENCRYPTION_PASSWORD = "encryptionPassword";
    private static final String PASSWORD_ENV_NAME = "passwordEnvName";
    private static final String PASSWORD_ALGORITHM = "encryptionAlgorithm";
    private static final String DEFAULT_PASSWORD_ENV_NAME = ActiveMQEncryptor.DEFAULT_PASSWORD_ENV_NAME;
    private static final String DEFAULT_PASSWORD_ALGORITHM = ActiveMQEncryptor.DEFAULT_LEGACY_ALGORITHM;

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {

        var encryptionPassword = (String) options.get(ENCRYPTION_PASSWORD);
        var passwordEnvName = options.get(PASSWORD_ENV_NAME) != null ?
                (String)options.get(PASSWORD_ENV_NAME) : DEFAULT_PASSWORD_ENV_NAME;
        var passwordAlgorithm = options.get(PASSWORD_ALGORITHM) != null ?
                (String)options.get(PASSWORD_ALGORITHM) : DEFAULT_PASSWORD_ALGORITHM;

        var encryptor = new ActiveMQEncryptor();
        encryptor.setLegacyAlgorithm(passwordAlgorithm);

        //If the password was set, use it
        //else look up the password from the environment
        if (encryptionPassword == null) {
            encryptor.setPasswordEnvName(passwordEnvName);
        } else {
            encryptor.setPassword(encryptionPassword);
        }

        var decryptedOptions = new HashMap(options);
        for (var entryObject : decryptedOptions.entrySet()) {
            var entry = (Map.Entry) entryObject;
            if (entry.getValue() instanceof String value
                    && ActiveMQEncryptor.isEncryptedValue(value)) {
                entry.setValue(encryptor.decrypt(value));
            }
        }

        super.initialize(subject, callbackHandler, sharedState, decryptedOptions);

    }

}
