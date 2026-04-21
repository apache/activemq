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
package org.apache.activemq.xbean;

import static org.apache.activemq.xbean.XBeanBrokerFactory.DEFAULT_ALLOWED_PROTOCOLS;
import static org.apache.activemq.xbean.XBeanBrokerFactory.XBEAN_BROKER_FACTORY_PROTOCOLS_PROP;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileNotFoundException;
import java.net.UnknownHostException;
import java.nio.file.NoSuchFileException;
import java.util.Set;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.spring.Utils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.FatalBeanException;

public class XBeanBrokerFactoryTest {

    @Before
    public void setUp() throws Exception {
        // reset before each test
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, DEFAULT_ALLOWED_PROTOCOLS);
    }

    @AfterClass
    public static void tearDown() throws Exception {
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, DEFAULT_ALLOWED_PROTOCOLS);
    }

    @Test
    public void testXBeanAllowedProtocolParsing() throws Exception {
        // new instance will read the current property protocol prop and build set
        XBeanBrokerFactory factory = new XBeanBrokerFactory();
        assertEquals(Set.of(Utils.FILE_PROTOCOL, Utils.CLASSPATH_PROTOCOL), factory.getAllowedProtocols());

        // set property
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "file,jar");
        factory = new XBeanBrokerFactory();
        assertEquals(Set.of("jar","file"), factory.getAllowedProtocols());
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "http");
        factory = new XBeanBrokerFactory();
        assertEquals(Set.of("http"), factory.getAllowedProtocols());

        // check allow all
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "*");
        factory = new XBeanBrokerFactory();
        assertNull(factory.getAllowedProtocols());

        // check allow none
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "");
        factory = new XBeanBrokerFactory();
        assertTrue(factory.getAllowedProtocols().isEmpty());

        // test empty and white space only
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "jar  , ftp,   http");
        factory = new XBeanBrokerFactory();
        assertEquals(Set.of("jar","ftp", "http"), factory.getAllowedProtocols());
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "   ");
        factory = new XBeanBrokerFactory();
        assertTrue(factory.getAllowedProtocols().isEmpty());
    }


    // Test default protocols
    @Test
    public void testDefaultXBeanProtocols() throws Exception {
        // file works
        startBroker("xbean:src/test/resources/spring/xbean-test.xml");
        startBroker("xbean:file:src/test/resources/spring/xbean-test.xml");

        // classpath works
        startBroker("xbean:spring/xbean-test.xml");
        startBroker("xbean:classpath:spring/xbean-test.xml");

        // http/fttp blocked by default
        startBrokerNotAllowedError("xbean:http://bad/xbean-test.xml");
        startBrokerNotAllowedError("xbean:ftp:bad/xbean-test.xml");
        // should get illegal state exception, we are not allowed to use the jar protocol
        startBrokerNotAllowedError("xbean:jar:file:invalid.jar!/");
        // remote file is blocked
        startBrokerNotAllowedError("xbean:file://remote/xbean-test.xml");

        // custom is not a known protocol so this is detected as invalid protocol
        // and not a URI, so it fallsback and tries classpath which is allowed
        // but won't be found
        startBrokerBeanError("xbean:custom://invalid", FileNotFoundException.class);
    }

    @Test
    public void testXBeanAllowNone() throws Exception {
        // Allow nothing
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "");

        // everything is not allowed
        startBrokerNotAllowedError("xbean:http://bad/xbean-test.xml",
                "No protocols are allowed for loading resources");
        startBrokerNotAllowedError("xbean:ftp:bad/xbean-test.xml",
                "No protocols are allowed for loading resources");
        startBrokerNotAllowedError("xbean:jar:file:invalid.jar!/",
                "No protocols are allowed for loading resources");
        startBrokerNotAllowedError("xbean:src/test/resources/spring/xbean-test.xml",
                "No protocols are allowed for loading resources");
        startBrokerNotAllowedError("xbean://src/test/resources/spring/xbean-test.xml",
                "No protocols are allowed for loading resources");
        startBrokerNotAllowedError("xbean:file:src/test/resources/spring/xbean-test.xml",
                "No protocols are allowed for loading resources");
        startBrokerNotAllowedError("xbean:file://src/test/resources/spring/xbean-test.xml",
                "No protocols are allowed for loading resources");
        startBrokerNotAllowedError("xbean:classpath:src/test/resources/spring/xbean-test.xml",
                "No protocols are allowed for loading resources");
    }

    @Test
    public void testXBeanAllowAll() throws Exception {
        // set to asterisk to allow all
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "*");

        // file works
        startBroker("xbean:src/test/resources/spring/xbean-test.xml");
        startBroker("xbean:file:src/test/resources/spring/xbean-test.xml");
        // classpath works
        startBroker("xbean:spring/xbean-test.xml");
        startBroker("xbean:classpath:spring/xbean-test.xml");
        // jar, http, ftp are allowed so they just get bean errors as can't be found
        startBrokerBeanError("xbean:jar:file:invalid.jar!/", NoSuchFileException.class);
        startBrokerBeanError("xbean:ftp://invalid", UnknownHostException.class);
        startBrokerBeanError("xbean:http://invalid", UnknownHostException.class);
        // check remote file too
        startBrokerBeanError("xbean:file://invalid", UnknownHostException.class);
    }

    @Test
    public void testDefaultXBeanProtocolsCustom() throws Exception {
        // update allowed list
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, "jar,ftp,http");

        // jar is allowed, but file is invalid so we get a different error
        startBrokerBeanError("xbean:jar:file:invalid.jar!/", NoSuchFileException.class);

        // ftp/http is allowed but url is invalid we get a different error
        // because the host is unknown
        startBrokerBeanError("xbean:ftp://invalid", UnknownHostException.class);
        startBrokerBeanError("xbean:http://invalid", UnknownHostException.class);

        // file, remote file and classpath are all blocked
        startBrokerNotAllowedError("xbean:src/test/resources/spring/xbean-test.xml",
                "can't be found or the protocol is not allowed");
        startBrokerNotAllowedError("xbean://remote/spring/xbean-test.xml",
                "can't be found or the protocol is not allowed");
        startBrokerNotAllowedError("xbean:file:src/test/resources/spring/xbean-test.xml");
        startBrokerNotAllowedError("xbean:file://remote/spring/xbean-test.xml");
        startBrokerNotAllowedError("xbean:spring/xbean-test.xml",
                "can't be found or the protocol is not allowed");
        startBrokerNotAllowedError("xbean:classpath:spring/xbean-test.xml");
    }

    @Test
    public void testXBeanProtocolsOnlyFileOrClasspath() throws Exception {
        // block classpath
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, Utils.FILE_PROTOCOL);

        // Files should work fine
        startBroker("xbean:src/test/resources/spring/xbean-test.xml");
        startBroker("xbean:file:src/test/resources/spring/xbean-test.xml");

        // Classpath entries won't work
        // not a URI and classpath isn't allowed so errors out
        startBrokerNotAllowedError("xbean:spring/xbean-test.xml", "can't be found or the protocol is not allowed");
        startBrokerNotAllowedError("xbean:classpath:spring/xbean-test.xml");

        // block files, allow classpath
        System.setProperty(XBEAN_BROKER_FACTORY_PROTOCOLS_PROP, Utils.CLASSPATH_PROTOCOL);

        // Files should now break
        // This will fallback to trying classpath because file isn't allowed and it isn't
        // a qualified URI so it will just get a bean exception as won't be on the classpath
        startBrokerBeanError("xbean:src/test/resources/spring/xbean-test.xml",
                FileNotFoundException.class);
        // qualified URI will try file no matter won't and will be blocked as file is not allowed
        // so it won't fall back
        startBrokerNotAllowedError("xbean:file:src/test/resources/spring/xbean-test.xml");

        // classpath should work
        startBroker("xbean:spring/xbean-test.xml");
        startBroker("xbean:classpath:spring/xbean-test.xml");
    }

    private void startBrokerBeanError(String url, Class<? extends Exception> expected) throws Exception {
        try {
            startBroker(url);
            fail("Should have failed with an exception");
        } catch (FatalBeanException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            assertTrue(expected.isAssignableFrom(cause.getClass()));
        }
    }

    private void startBrokerNotAllowedError(String url) throws Exception {
        startBrokerNotAllowedError(url, "which is not allowed for loading URL resources");
    }

    private void startBrokerNotAllowedError(String url, String expected) throws Exception {
        try {
            startBroker(url);
            fail("Should have failed with an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(expected));
        }
    }

    private void startBroker(String url) throws Exception {
        BrokerService broker = null;
        try {
            broker = BrokerFactory.createBroker(url);
            assertNotNull(broker);
            broker.stop();
        } finally {
            if (broker != null) {
                broker.stop();
                broker.waitUntilStopped();
            }
        }
    }
}
