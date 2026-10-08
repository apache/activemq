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
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.context.support.PropertySourcesPlaceholderConfigurer;
import org.springframework.core.convert.support.ConfigurableConversionService;
import org.springframework.core.env.ConfigurablePropertyResolver;
import org.springframework.core.env.MissingRequiredPropertiesException;

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
    protected void processProperties(ConfigurableListableBeanFactory beanFactoryToProcess,
            ConfigurablePropertyResolver propertyResolver) throws BeansException {
        super.processProperties(beanFactoryToProcess, new DecryptingPropertyResolver(propertyResolver));
    }

    private String decryptIfEncrypted(String value) {
        if (value != null && ActiveMQEncryptor.isEncryptedValue(value)) {
            return encryptor.decrypt(value);
        }
        return value;
    }

    /**
     * Delegates to the resolver built by the parent class and decrypts
     * resolved values that are entirely an {@code ENC(...)} token.
     */
    private class DecryptingPropertyResolver implements ConfigurablePropertyResolver {

        private final ConfigurablePropertyResolver delegate;

        DecryptingPropertyResolver(ConfigurablePropertyResolver delegate) {
            this.delegate = delegate;
        }

        @Override
        public String resolvePlaceholders(String text) {
            return decryptIfEncrypted(delegate.resolvePlaceholders(text));
        }

        @Override
        public String resolveRequiredPlaceholders(String text) throws IllegalArgumentException {
            return decryptIfEncrypted(delegate.resolveRequiredPlaceholders(text));
        }

        @Override
        public String getProperty(String key) {
            return decryptIfEncrypted(delegate.getProperty(key));
        }

        @Override
        public String getProperty(String key, String defaultValue) {
            return decryptIfEncrypted(delegate.getProperty(key, defaultValue));
        }

        @Override
        public String getRequiredProperty(String key) throws IllegalStateException {
            return decryptIfEncrypted(delegate.getRequiredProperty(key));
        }

        @Override
        public boolean containsProperty(String key) {
            return delegate.containsProperty(key);
        }

        @Override
        public <T> T getProperty(String key, Class<T> targetType) {
            return delegate.getProperty(key, targetType);
        }

        @Override
        public <T> T getProperty(String key, Class<T> targetType, T defaultValue) {
            return delegate.getProperty(key, targetType, defaultValue);
        }

        @Override
        public <T> T getRequiredProperty(String key, Class<T> targetType) throws IllegalStateException {
            return delegate.getRequiredProperty(key, targetType);
        }

        @Override
        public ConfigurableConversionService getConversionService() {
            return delegate.getConversionService();
        }

        @Override
        public void setConversionService(ConfigurableConversionService conversionService) {
            delegate.setConversionService(conversionService);
        }

        @Override
        public void setPlaceholderPrefix(String placeholderPrefix) {
            delegate.setPlaceholderPrefix(placeholderPrefix);
        }

        @Override
        public void setPlaceholderSuffix(String placeholderSuffix) {
            delegate.setPlaceholderSuffix(placeholderSuffix);
        }

        @Override
        public void setValueSeparator(String valueSeparator) {
            delegate.setValueSeparator(valueSeparator);
        }

        @Override
        public void setEscapeCharacter(Character escapeCharacter) {
            delegate.setEscapeCharacter(escapeCharacter);
        }

        @Override
        public void setIgnoreUnresolvableNestedPlaceholders(boolean ignoreUnresolvableNestedPlaceholders) {
            delegate.setIgnoreUnresolvableNestedPlaceholders(ignoreUnresolvableNestedPlaceholders);
        }

        @Override
        public void setRequiredProperties(String... requiredProperties) {
            delegate.setRequiredProperties(requiredProperties);
        }

        @Override
        public void validateRequiredProperties() throws MissingRequiredPropertiesException {
            delegate.validateRequiredProperties();
        }
    }
}
