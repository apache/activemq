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
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.env.ConfigurablePropertyResolver;
import org.springframework.core.env.MutablePropertySources;
import org.springframework.core.env.PropertySource;

/**
 * Property placeholder configurer that resolves {@code ENC(...)} values with
 * an {@link ActiveMQEncryptor}. This is the
 * {@link PropertySourcesPlaceholderConfigurer} based variant and the
 * recommended configurer for new configurations; placeholders resolve
 * against the full Spring Environment (system properties and environment
 * variables) in addition to the configured properties files.
 *
 * <pre>{@code
 * <bean id="configurationEncryptor" class="org.apache.activemq.util.ActiveMQEncryptor">
 *     <property name="passwordEnvName" value="ACTIVEMQ_ENCRYPTION_PASSWORD"/>
 * </bean>
 * <bean id="propertyConfigurer" class="org.apache.activemq.spring.EncryptablePropertySourcesPlaceholderConfigurer">
 *     <constructor-arg ref="configurationEncryptor"/>
 *     <property name="localOverride" value="true"/>
 *     <property name="location" value="file:${activemq.conf}/credentials-enc.properties"/>
 * </bean>
 * }</pre>
 *
 * <p>Set {@code localOverride=true} to keep the precedence previous releases
 * had: values from the configured properties files win over system
 * properties and environment variables.
 *
 * <p>Values encrypted by the jasypt-based tooling of previous releases are
 * still resolved; see {@link ActiveMQEncryptor} for details.
 */
public class EncryptablePropertySourcesPlaceholderConfigurer extends PropertySourcesPlaceholderConfigurer {

    private final TextEncryptor encryptor;

    public EncryptablePropertySourcesPlaceholderConfigurer() {
        this(new ActiveMQEncryptor());
    }

    public EncryptablePropertySourcesPlaceholderConfigurer(TextEncryptor encryptor) {
        if (encryptor == null) {
            throw new IllegalArgumentException("encryptor must not be null");
        }
        this.encryptor = encryptor;
    }

    @Override
    protected ConfigurablePropertyResolver createPropertyResolver(MutablePropertySources propertySources) {
        var decrypting = new MutablePropertySources();
        for (var source : propertySources) {
            decrypting.addLast(new DecryptingPropertySource(source));
        }
        return super.createPropertyResolver(decrypting);
    }

    // decrypts each property value, so a placeholder inside a larger value gets the plaintext
    private class DecryptingPropertySource extends PropertySource<PropertySource<?>> {

        DecryptingPropertySource(PropertySource<?> source) {
            super(source.getName(), source);
        }

        @Override
        public Object getProperty(String name) {
            var value = source.getProperty(name);
            if (value instanceof String text && ActiveMQEncryptor.isEncryptedValue(text)) {
                return encryptor.decrypt(ActiveMQEncryptor.unwrapEncryptedValue(text));
            }
            return value;
        }

        @Override
        public boolean containsProperty(String name) {
            return source.containsProperty(name);
        }
    }
}
