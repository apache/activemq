/*
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

import static org.apache.activemq.xbean.XBeanBrokerFactory.DEFAULT_ALLOWED_PROTOCOLS;
import static org.apache.activemq.xbean.XBeanBrokerFactory.XBEAN_BROKER_FACTORY_PROTOCOLS_PROP;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import jakarta.jms.Connection;
import jakarta.jms.JMSException;
import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.nio.file.NoSuchFileException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.test.annotations.ParallelTest;
import org.apache.activemq.xbean.XBeanBrokerFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.beans.FatalBeanException;

@Category(ParallelTest.class)
public class ActiveMQConnectionFactoryXBeanTest {

    @Before
    public void setUp() throws Exception {
        // reset before each test
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, DEFAULT_ALLOWED_PROTOCOLS);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, DEFAULT_ALLOWED_PROTOCOLS);
    }

    // File and classpath are allowed by default
    @Test
    public void testCreateBrokerDefaults() throws Exception {
        // File resources
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:src/test/resources/activemq.xml");
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:file:src/test/resources/activemq.xml");

        // Classpath resources
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:activemq.xml");
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:classpath:activemq.xml");

        // Remote file not allowed by default, falls back to classpath as it isn't fully qualified
        assertBrokerStartError("vm://localhost?brokerConfig=xbean://activemq.xml",
                FileNotFoundException.class);
        // Remote file not allowed by default
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:file://activemq.xml");

        // other URL types blocked
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:http://activemq.xml");
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:ftp://activemq.xml");
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:jar:file:invalid.jar!/");

        // Custom is not a valid protocol so this does not get processed as a URI
        // so classpath is tried (as it is allowed and gets tried last with no uri
        // prefix). Spring won't find the file on the classpath.
        assertBrokerStartError("vm://localhost?brokerConfig=xbean:custom:activemq.xml",
                FileNotFoundException.class);
    }

    @Test
    public void testCreateBrokerAllowNone() {
        // empty allows none
        System.setProperty(XBeanBrokerFactory.XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "");

        // File resources blocked
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:src/test/resources/activemq.xml",
                "No protocols are allowed for loading resources");
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:file:src/test/resources/activemq.xml",
                "No protocols are allowed for loading resources");

        // Remote file resources blocked
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean://remote/resources/activemq.xml",
                "No protocols are allowed for loading resources");
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:file://remote/resources/activemq.xml",
                "No protocols are allowed for loading resources");

        // Classpath resources blocked
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:activemq.xml",
                "No protocols are allowed for loading resources");
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:classpath:activemq.xml",
                "No protocols are allowed for loading resources");

        //others blocked, we get IllegalArgumentException without trying to load
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:http://invalid",
                "No protocols are allowed for loading resources");
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:ftp://invalid",
                "No protocols are allowed for loading resources");
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:jar:file:invalid.jar!/",
                "No protocols are allowed for loading resources");
    }

    // File and classpath are allowed by default
    @Test
    public void testCreateBrokerAllowAll() throws Exception {
        // allow all with asterisk
        System.setProperty(XBeanBrokerFactory.XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "*");

        // File resources
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:src/test/resources/activemq.xml");
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:file:src/test/resources/activemq.xml");

        // Classpath resources
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:activemq.xml");
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:classpath:activemq.xml");

        // http/ftp allowed but unknown host
        assertBrokerStartError("vm://localhost?brokerConfig=xbean:http://invalid",
                UnknownHostException.class);
        assertBrokerStartError("vm://localhost?brokerConfig=xbean:ftp://invalid",
                UnknownHostException.class);
    }

    @Test
    public void testCreateBrokerPropConfigured() throws Exception {
        // allow only file and jar
        System.setProperty(XBeanBrokerFactory.XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "file,jar,ftp");

        // File resources - allowed
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:src/test/resources/activemq.xml");
        assertBrokerCreated("vm://localhost?brokerConfig=xbean:file:src/test/resources/activemq.xml");

        // Remote file resources - not allowed
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean://activemq.xml",
                "can't be found or is not allowed");
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:file://activemq.xml");

        // Classpath resources - not allowed
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:activemq.xml",
                "can't be found or is not allowed");
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:classpath:activemq.xml");

        // http not allowed
        assertUrlNotAllowed("vm://localhost?brokerConfig=xbean:http://invalid");

        // ftp is allowed, but can't be found with bad host
        assertBrokerStartError("vm://localhost?brokerConfig=xbean:ftp://invalid",
                UnknownHostException.class);

        // jar is now allowed but file doesn't exist
        assertBrokerStartError("vm://localhost?brokerConfig=xbean:jar:file:invalid.jar!/", NoSuchFileException.class);

        System.setProperty(XBeanBrokerFactory.XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "remote-file,jar,ftp");
        // remote file is now allowed, but can't be found with bad host
        assertBrokerStartError("vm://localhost?brokerConfig=xbean:file://invalid",
                UnknownHostException.class);
    }

    private void assertBrokerCreated(String url) throws JMSException {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        try (Connection connection = factory.createConnection()) {
            assertNotNull(connection);
        }
    }

    private void assertUrlNotAllowed(String url) {
        assertUrlNotAllowed(url, " which is not allowed for loading URL resources");
    }

    private void assertUrlNotAllowed(String url, String error) {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        try (Connection ignored = factory.createConnection()) {
            fail("Should of thrown exception");
        } catch (JMSException e) {
            assertTrue(e.getCause() instanceof IllegalArgumentException);
            assertTrue(e.getMessage().contains(error));
        }
    }

    private void assertBrokerStartError(String url, Class<? extends Exception> expected) {
        final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
        try (Connection ignored = factory.createConnection()) {
            fail("Should have failed with an exception");
        } catch (JMSException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            assertTrue(cause instanceof FatalBeanException);
            cause = cause.getCause() != null ? cause.getCause() : cause;
            assertTrue(expected.isAssignableFrom(cause.getClass()));
        }
    }
}
