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
package org.apache.activemq.spring;

import org.apache.activemq.util.ActiveMQEncryptor;
import org.apache.activemq.util.TextEncryptor;
import org.springframework.beans.factory.config.PropertyPlaceholderConfigurer;

/**
 * Property placeholder configurer that resolves {@code ENC(...)} values with
 * an {@link ActiveMQEncryptor}. Drop-in replacement for the jasypt
 * {@code EncryptablePropertyPlaceholderConfigurer} used by previous
 * releases:
 *
 * <pre>{@code
 * <bean id="configurationEncryptor" class="org.apache.activemq.util.ActiveMQEncryptor">
 *     <property name="passwordEnvName" value="ACTIVEMQ_ENCRYPTION_PASSWORD"/>
 * </bean>
 * <bean id="propertyConfigurer" class="org.apache.activemq.spring.EncryptablePropertyPlaceholderConfigurer">
 *     <constructor-arg ref="configurationEncryptor"/>
 *     <property name="location" value="file:${activemq.conf}/credentials-enc.properties"/>
 * </bean>
 * }</pre>
 *
 * Values encrypted by the jasypt-based tooling of previous releases are
 * still resolved; see {@link ActiveMQEncryptor} for details.
 */
@SuppressWarnings("deprecation")
public class EncryptablePropertyPlaceholderConfigurer extends PropertyPlaceholderConfigurer {

    private final TextEncryptor encryptor;

    public EncryptablePropertyPlaceholderConfigurer() {
        this(new ActiveMQEncryptor());
    }

    public EncryptablePropertyPlaceholderConfigurer(TextEncryptor encryptor) {
        if (encryptor == null) {
            throw new IllegalArgumentException("encryptor must not be null");
        }
        this.encryptor = encryptor;
    }

    @Override
    protected String convertPropertyValue(String originalValue) {
        if (ActiveMQEncryptor.isEncryptedValue(originalValue)) {
            return encryptor.decrypt(originalValue);
        }
        return originalValue;
    }

    @Override
    protected String resolveSystemProperty(String key) {
        var value = super.resolveSystemProperty(key);
        if (value != null && ActiveMQEncryptor.isEncryptedValue(value)) {
            return encryptor.decrypt(value);
        }
        return value;
    }
}
