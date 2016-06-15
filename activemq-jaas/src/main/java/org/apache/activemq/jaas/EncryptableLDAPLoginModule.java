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

import java.util.Map;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;

import org.jasypt.encryption.pbe.StandardPBEStringEncryptor;
import org.jasypt.encryption.pbe.config.EnvironmentStringPBEConfig;
import org.jasypt.properties.EncryptableProperties;

/**
 * LDAPLoginModule that supports encryption
 */
public class EncryptableLDAPLoginModule extends LDAPLoginModule {

    private static final String ENCRYPTION_PASSWORD = "encryptionPassword";
    private static final String PASSWORD_ENV_NAME = "passwordEnvName";
    private static final String PASSWORD_ALGORITHM = "encryptionAlgorithm";
    private static final String DEFAULT_PASSWORD_ENV_NAME = "ACTIVEMQ_ENCRYPTION_PASSWORD";
    private static final String DEFAULT_PASSWORD_ALGORITHM = "PBEWithMD5AndDES";
    private final StandardPBEStringEncryptor configurationEncryptor = new StandardPBEStringEncryptor();

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {

        String encryptionPassword = (String)options.get(ENCRYPTION_PASSWORD);
        String passwordEnvName = options.get(PASSWORD_ENV_NAME) != null ?
                (String)options.get(PASSWORD_ENV_NAME) : DEFAULT_PASSWORD_ENV_NAME;
        String passwordAlgorithm = options.get(PASSWORD_ALGORITHM) != null ?
                (String)options.get(PASSWORD_ALGORITHM) : DEFAULT_PASSWORD_ALGORITHM;

        EnvironmentStringPBEConfig envConfig = new EnvironmentStringPBEConfig();
        envConfig.setAlgorithm(passwordAlgorithm);

        //If the password was set, use it
        //else look up the password from the environment
        if (encryptionPassword == null) {
            envConfig.setPasswordEnvName(passwordEnvName);
        } else {
            envConfig.setPassword(encryptionPassword);
        }

        configurationEncryptor.setConfig(envConfig);
        EncryptableProperties encryptableOptions
            = new EncryptableProperties(configurationEncryptor);
        encryptableOptions.putAll(options);

        super.initialize(subject, callbackHandler, sharedState, encryptableOptions);

    }

}
