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

import static org.junit.Assert.assertEquals;

import java.util.Properties;

import org.apache.activemq.util.ActiveMQEncryptor;
import org.junit.Test;
import org.springframework.beans.factory.config.PlaceholderConfigurerSupport;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.context.support.GenericApplicationContext;

/**
 * Verifies that both encryptable placeholder configurers resolve plaintext,
 * legacy jasypt-format and current-format ENC() values from a properties
 * file into bean properties.
 */
public class EncryptablePlaceholderConfigurerTest {

    @Test
    public void testPropertyPlaceholderConfigurerVariant() {
        assertResolves("org/apache/activemq/spring/encryptable-ppc-context.xml");
    }

    @Test
    public void testPropertySourcesPlaceholderConfigurerVariant() {
        assertResolves("org/apache/activemq/spring/encryptable-pspc-context.xml");
    }

    @Test
    public void testPropertyPlaceholderConfigurerResolvesEncryptedValueInsideAnother() {
        assertEncryptedValueInsideAnotherResolves(new EncryptablePropertyPlaceholderConfigurer(newEncryptor()));
    }

    @Test
    public void testPropertySourcesPlaceholderConfigurerResolvesEncryptedValueInsideAnother() {
        assertEncryptedValueInsideAnotherResolves(new EncryptablePropertySourcesPlaceholderConfigurer(newEncryptor()));
    }

    private static ActiveMQEncryptor newEncryptor() {
        var encryptor = new ActiveMQEncryptor();
        encryptor.setPassword("activemq");
        return encryptor;
    }

    private static void assertEncryptedValueInsideAnotherResolves(PlaceholderConfigurerSupport configurer) {
        var encryptor = newEncryptor();
        var properties = new Properties();
        properties.setProperty("plain.value", "plaintext");
        properties.setProperty("legacy.secret", "ENC(4Pp68aOwkNEyvCcPBDCJAw==)");
        properties.setProperty("secret", ActiveMQEncryptor.wrapEncryptedValue(encryptor.encrypt("password")));
        properties.setProperty("pointer", ActiveMQEncryptor.wrapEncryptedValue(encryptor.encrypt("plain.value")));
        properties.setProperty("url", "tcp://${secret}@host");

        var target = resolve(configurer, properties,
            "prefix-${legacy.secret}-${secret}-suffix", "${${pointer}}", "${url}");

        assertEquals("prefix-manager-password-suffix", target.getPlain());
        assertEquals("plaintext", target.getLegacy());
        assertEquals("tcp://password@host", target.getCurrent());
    }

    private static TargetBean resolve(PlaceholderConfigurerSupport configurer, Properties properties,
            String plain, String legacy, String current) {
        configurer.setProperties(properties);
        try (var context = new GenericApplicationContext()) {
            context.addBeanFactoryPostProcessor(configurer);
            context.registerBeanDefinition("target", BeanDefinitionBuilder.genericBeanDefinition(TargetBean.class)
                .addPropertyValue("plain", plain)
                .addPropertyValue("legacy", legacy)
                .addPropertyValue("current", current)
                .getBeanDefinition());
            context.refresh();
            return context.getBean(TargetBean.class);
        }
    }

    private void assertResolves(String contextResource) {
        try (var context = new ClassPathXmlApplicationContext(contextResource)) {
            var target = context.getBean("target", TargetBean.class);
            assertEquals("plaintext", target.getPlain());
            assertEquals("manager", target.getLegacy());
            assertEquals("password", target.getCurrent());
        }
    }

    public static class TargetBean {

        private String plain;
        private String legacy;
        private String current;

        public String getPlain() {
            return plain;
        }

        public void setPlain(String plain) {
            this.plain = plain;
        }

        public String getLegacy() {
            return legacy;
        }

        public void setLegacy(String legacy) {
            this.legacy = legacy;
        }

        public String getCurrent() {
            return current;
        }

        public void setCurrent(String current) {
            this.current = current;
        }
    }
}
