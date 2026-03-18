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
package org.apache.activemq.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.activemq.transport.TransportFactory;
import org.apache.activemq.transport.nio.NIOTransportFactory;
import org.apache.activemq.transport.tcp.SslTransport;
import org.apache.activemq.transport.tcp.SslTransportFactory;
import org.apache.activemq.transport.tcp.TcpTransportFactory;
import org.apache.activemq.util.FactoryFinder.ObjectFactory;
import org.apache.activemq.wireformat.WireFormatFactory;
import org.junit.Test;

public class FactoryFinderTest {

    private static final String TRANSPORT_FACTORY_PATH = "META-INF/services/org/apache/activemq/transport/";
    private static final String WIREFORMAT_FACTORY_PATH = "META-INF/services/org/apache/activemq/wireformat/";

    // Test path traversal attempts will throw an error
    @Test
    public void testPathTraversal() throws Exception {
        FactoryFinder<TransportFactory> finder
                = new FactoryFinder<>(TRANSPORT_FACTORY_PATH, TransportFactory.class, null);
        assertNull(finder.getAllowedImpls());

        try {
            finder.newInstance("../../tcp");
            fail("should have failed instantiation");
        } catch (InstantiationException e) {
            assertEquals("Provided key escapes the FactoryFinder configured directory",
                    e.getMessage());
        }
    }

    // WireFormatFactory is not assignable to TransportFactory
    // So the constructor should throw an IllegalArgumentException
    @Test(expected = IllegalArgumentException.class)
    public void testAllowedImplsMatchInterface() {
        new FactoryFinder<>(TRANSPORT_FACTORY_PATH, TransportFactory.class,
                FactoryFinder.buildAllowedImpls(WireFormatFactory.class));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAllowedImplsNotFound() {
        new FactoryFinder<>(TRANSPORT_FACTORY_PATH, TransportFactory.class, "some.invalid.ClassName");
    }

    @Test(expected = IOException.class)
    public void testLoadClassNull() throws IOException, ClassNotFoundException {
        FactoryFinder.loadClass(null);
    }

    @Test(expected = ClassNotFoundException.class)
    public void testLoadClassNotFound() throws IOException, ClassNotFoundException {
        FactoryFinder.loadClass("some.invalid.ClassName");
    }

    // The default factory should throw UnsupportedOperationException
    // if using legacy create() method
    @Test(expected = UnsupportedOperationException.class)
    public void testDefaultObjectFinder() throws Exception {
        ObjectFactory factory = FactoryFinder.getObjectFactory();
        factory.create("path");
    }

    @Test
    public void testObjectFinderDefaultMethod() throws Exception {
        AtomicBoolean called = new AtomicBoolean();
        ObjectFactory factory = path -> {
            called.set(true);
            return null;
        };
        // test that new method defaults to legacy method if not implemented
        factory.create(null, null, null);
        assertTrue(called.get());
    }

    // Verify interface check works if not using an allowed list to at least verify that
    // any classes loaded will implement/extend the required type
    @Test
    public void testInterfaceMismatch() throws Exception {
        // use wrong interface type, WIREFORMAT_FACTORY_PATH should be of type WireFormatFactory
        // and not TransportFactory
        FactoryFinder<TransportFactory> factory = new FactoryFinder<>(WIREFORMAT_FACTORY_PATH, TransportFactory.class,
                null);
        // This is a valid impl in the wireformat directory, but it isn't a TransportFactory type
        try {
            factory.newInstance("default");
        } catch (InstantiationException e) {
            assertTrue(e.getMessage().contains("OpenWireFormatFactory is not assignable to class " +
                    TransportFactory.class.getName()));
        }
    }

    // Test filtering by allowed implementations of the given interface
    @Test
    public void testAllowedImplsFilter() throws Exception {
        Set<Class<?>> allowedImpls = Set.of(TcpTransportFactory.class, NIOTransportFactory.class);
        FactoryFinder<TransportFactory> finder = new FactoryFinder<>(TRANSPORT_FACTORY_PATH, TransportFactory.class,
                FactoryFinder.buildAllowedImpls(allowedImpls.toArray(new Class<?>[0])));
        assertEquals(TransportFactory.class, finder.getRequiredType());
        assertEquals(allowedImpls, finder.getAllowedImpls());
        // tcp and nio are the only class in the allow list
        assertNotNull(finder.newInstance("tcp"));

        try {
            // ssl transport factory was not in the allow list
            finder.newInstance("ssl");
            fail("should have failed instantiation");
        } catch (InstantiationException e) {
            assertTrue(e.getMessage().contains("is not an allowed implementation of type"));
        }
    }

    // empty array is used so we should deny everything
    @Test
    public void testAllowedImplsFilterDenyAll() throws Exception {
        FactoryFinder<TransportFactory> finder = new FactoryFinder<>(TRANSPORT_FACTORY_PATH,
                TransportFactory.class, "");
        assertEquals(TransportFactory.class, finder.getRequiredType());
        assertEquals(Set.of(), finder.getAllowedImpls());

        try {
            // nothing allowed, tcp exists but should be blocked
            finder.newInstance("tcp");
            fail("should have failed instantiation");
        } catch (InstantiationException e) {
            assertTrue(e.getMessage().contains("is not an allowed implementation of type"));
        }
    }

    // Test that impls are not filtered if the list is null
    @Test
    public void testAllowedImplsFilterAllowAll() throws Exception {
        // check with constructor that should default to null
        FactoryFinder<TransportFactory> finder = new FactoryFinder<>(TRANSPORT_FACTORY_PATH,
                TransportFactory.class, null);
        assertNull(finder.getAllowedImpls());
        assertNotNull(finder.newInstance("tcp"));
        assertNotNull(finder.newInstance("ssl"));

        // check again with explicit null
        finder = new FactoryFinder<>(TRANSPORT_FACTORY_PATH,
                TransportFactory.class, null);
        assertNull(finder.getAllowedImpls());
        assertNotNull(finder.newInstance("tcp"));
        assertNotNull(finder.newInstance("ssl"));

        try {
            // abc is allowed because we are not filtering by allowed impls but
            // we should not be able to find it
            finder.newInstance("abc");
            fail("should have failed instantiation");
        } catch (IOException e) {
            assertTrue(e.getMessage().contains("Could not find factory class for resource"));
        }
    }

    @Test
    public void testAllowedImplsTrimWhiteSpace() throws Exception {
        // Build CSV with white space between all classes, including two white space
        // only elements
        String implsWithWhiteSpace = String.join(",   ",
                TcpTransportFactory.class.getName(), SslTransportFactory.class.getName(),
                " ", NIOTransportFactory.class.getName(), "");

        FactoryFinder<TransportFactory> finder = new FactoryFinder<>(TRANSPORT_FACTORY_PATH,
                TransportFactory.class, implsWithWhiteSpace);

        // all white space should have been trimmed and all empty values skipped
        // leading to the 3 valid classes loaded
        assertEquals(Set.of(TcpTransportFactory.class, SslTransportFactory.class,
                NIOTransportFactory.class), finder.getAllowedImpls());
        assertNotNull(finder.newInstance("tcp"));
        assertNotNull(finder.newInstance("ssl"));
        assertNotNull(finder.newInstance("nio"));
    }
}
