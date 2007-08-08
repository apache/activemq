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

import org.apache.activemq.broker.BrokerService;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public class SpringTest extends SpringTestSupport {
    /**
     * Make sure that brokers are being pooled properly.
     *
     * @throws Exception
     */
    public void testSenderWithSpringXmlEmbeddedPooledBrokerConfiguredViaXml() throws Exception {
        String config = "spring-embedded-pooled.xml";

        Thread.currentThread().setContextClassLoader(SpringTest.class.getClassLoader());
        ClassPathXmlApplicationContext context1 = new ClassPathXmlApplicationContext(config);

        BrokerService bs1 = (BrokerService) context1.getBean("broker1");
        assertNotNull(bs1);
        BrokerService bs2 = (BrokerService) context1.getBean("broker2");
        assertNotNull(bs1);

        // It should be the same broker;
        assertEquals(bs1, bs2);

        // Even if we load up another context, it should still be the same broker.
        ClassPathXmlApplicationContext context2 = new ClassPathXmlApplicationContext(config);

        BrokerService bs3 = (BrokerService) context2.getBean("broker1");
        assertNotNull(bs3);
        BrokerService bs4 = (BrokerService) context2.getBean("broker2");
        assertNotNull(bs4);

        // It should be the same broker;
        assertEquals(bs1, bs3);
        assertEquals(bs1, bs4);

        // And it should be started.
        assertTrue(bs1.isStarted());

        // should still be started asfter the 2nd context closes.
        context2.close();
        assertTrue(bs1.isStarted());

        // Should stop once all contexts close.
        context1.close();
        assertFalse(bs1.isStarted());
    }

    /**
     * Uses ActiveMQConnectionFactory to create the connection context.
     * Configuration file is /resources/spring.xml
     *
     * @throws Exception
     */
    public void testSenderWithSpringXml() throws Exception {
        String config = "spring.xml";
        assertSenderConfig(config);
    }

    /**
     * Spring configured test that uses ActiveMQConnectionFactory for
     * connection context and ActiveMQQueue for destination. Configuration
     * file is /resources/spring-queue.xml.
     *
     * @throws Exception
     */
    public void testSenderWithSpringXmlAndQueue() throws Exception {
        String config = "spring-queue.xml";
        assertSenderConfig(config);
    }

    /**
     * Spring configured test that uses JNDI. Configuration file is
     * /resources/spring-jndi.xml.
     *
     * @throws Exception
     */
    public void testSenderWithSpringXmlUsingJNDI() throws Exception {
        String config = "spring-jndi.xml";
        assertSenderConfig(config);
    }

    /**
     * Spring configured test where in the connection context is set to use
     * an embedded broker. Configuration file is /resources/spring-embedded.xml
     * and /resources/activemq.xml.
     *
     * @throws Exception
     */
    public void testSenderWithSpringXmlEmbeddedBrokerConfiguredViaXml() throws Exception {
        String config = "spring-embedded.xml";
        assertSenderConfig(config);
    }

    /**
     * Spring configured test case that tests the remotely deployed xsd
     * http://people.apache.org/repository/org.apache.activemq/xsds/activemq-core-4.1-SNAPSHOT.xsd
     *
     * @throws Exception
     */
    public void testSenderWithSpringXmlUsingSpring2NamespacesWithEmbeddedBrokerConfiguredViaXml() throws Exception {
        String config = "spring-embedded-xbean.xml";
        assertSenderConfig(config);
    }

    /**
     * Spring configured test case that tests the locally generated xsd
     *
     * @throws Exception
     */
    public void testSenderWithSpringXmlUsingSpring2NamespacesWithEmbeddedBrokerConfiguredViaXmlUsingLocalXsd() throws Exception {
        String config = "spring-embedded-xbean-local.xml";
        assertSenderConfig(config);
    }
}
