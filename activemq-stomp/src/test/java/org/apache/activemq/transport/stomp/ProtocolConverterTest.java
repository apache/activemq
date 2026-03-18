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
package org.apache.activemq.transport.stomp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import org.apache.activemq.transport.stomp.StompTest.TestTranslator;
import org.apache.activemq.util.FactoryFinder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class ProtocolConverterTest {

    private final StompTransport transport = mock(StompTransport.class);
    // Before each test reset to defaults
    @Before
    public void setUp() {
        resetDefaultAllowedTranslators();
    }

    // clean up after all tests are done finished by resetting the property back before exiting
    @AfterClass
    public static void tearDown() {
        resetDefaultAllowedTranslators();
    }

    @Test
    public void testLoadMissingTranslator() throws Exception {
        ProtocolConverter converter = new ProtocolConverter(transport, null);

        // Should fallback to default
        assertEquals(LegacyFrameTranslator.class, converter.findTranslator("abc").getClass());

        try {
            converter.loadTranslator("abc");
            fail("should have failed");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Could not find factory class for resource"));
        }
    }

    @Test
    public void testAllowedTranslators() throws Exception {
        // first test defaults
        ProtocolConverter converter = new ProtocolConverter(transport, null);
        assertNotNull(converter.loadTranslator("jms-json"));
        assertEquals(JmsFrameTranslator.class, converter.findTranslator("jms-json").getClass());
        assertNotNull(converter.loadTranslator("jms-xml"));
        assertEquals(JmsFrameTranslator.class, converter.findTranslator("jms-xml").getClass());

        // test specific allowed
        System.setProperty(ProtocolConverter.FRAME_TRANSLATOR_CLASSES_PROP,
                JmsFrameTranslator.class.getName());
        converter = new ProtocolConverter(transport, null);
        assertEquals(JmsFrameTranslator.class, converter.findTranslator("jms-json").getClass());
        assertEquals(JmsFrameTranslator.class, converter.findTranslator("jms-byte").getClass());

        // test all allowed
        System.setProperty(ProtocolConverter.FRAME_TRANSLATOR_CLASSES_PROP, "*");
        converter = new ProtocolConverter(transport, null);
        assertEquals(JmsFrameTranslator.class, converter.findTranslator("jms-json").getClass());
        assertEquals(JmsFrameTranslator.class, converter.findTranslator("jms-byte").getClass());
        // our custom is also allowed
        assertNotNull(converter.loadTranslator("stomp-test"));
        assertEquals(TestTranslator.class, converter.findTranslator("stomp-test").getClass());
    }

    @Test
    public void testAllowedTranslatorsErrors() throws Exception {
        // Disable all and allow none
        System.setProperty(ProtocolConverter.FRAME_TRANSLATOR_CLASSES_PROP, "");
        ProtocolConverter converter = new ProtocolConverter(transport, null);

        assertTranslatorNotAllowed(converter, "jms-json");

        // should fall back to default legacy on error
        assertEquals(LegacyFrameTranslator.class, converter.findTranslator("jms-json").getClass());

        // test custom translator not in allowed list
        System.setProperty(ProtocolConverter.FRAME_TRANSLATOR_CLASSES_PROP, JmsFrameTranslator.class.getName());
        converter = new ProtocolConverter(transport, null);
        assertTranslatorNotAllowed(converter, "stomp-test");

    }

    @Test
    public void testCustomAllowedTranslator() throws Exception {
        System.setProperty(ProtocolConverter.FRAME_TRANSLATOR_CLASSES_PROP,
                FactoryFinder.buildAllowedImpls(JmsFrameTranslator.class, TestTranslator.class));
        ProtocolConverter converter = new ProtocolConverter(transport, null);

        // JmsFrameTranslator is allowed
        assertNotNull(converter.loadTranslator("jms-json"));
        assertEquals(JmsFrameTranslator.class, converter.findTranslator("jms-json").getClass());

        // our custom is also allowed
        assertNotNull(converter.loadTranslator("stomp-test"));
        assertEquals(TestTranslator.class, converter.findTranslator("stomp-test").getClass());
    }

    @Test
    public void testWhiteSpaceAllowedTranslator() throws Exception {
        // white space should be trimmed
        System.setProperty(ProtocolConverter.FRAME_TRANSLATOR_CLASSES_PROP,
                String.join(",   ", JmsFrameTranslator.class.getName(),
                        TestTranslator.class.getName()));
        ProtocolConverter converter = new ProtocolConverter(transport, null);

        // JmsFrameTranslator is allowed
        assertNotNull(converter.loadTranslator("jms-json"));
        assertEquals(JmsFrameTranslator.class, converter.findTranslator("jms-json").getClass());

        // our custom is also allowed
        assertNotNull(converter.loadTranslator("stomp-test"));
        assertEquals(TestTranslator.class, converter.findTranslator("stomp-test").getClass());
    }

    @Test
    public void testMissingInterface() throws Exception {
        System.setProperty(ProtocolConverter.FRAME_TRANSLATOR_CLASSES_PROP, "*");
        ProtocolConverter converter = new ProtocolConverter(transport, null);

        try {
            converter.loadTranslator("stomp-test-invalid");
            fail("should have failed");
        } catch (InstantiationException e) {
            assertTrue(e.getMessage().contains(BadTranslator.class.getName() + " is not assignable to interface "
                    + FrameTranslator.class.getName()));
        }

        // fallback
        assertEquals(LegacyFrameTranslator.class, converter.findTranslator("stomp-test-invalid").getClass());
    }

    @Test
    public void testPathTraversal() throws Exception {
        ProtocolConverter converter = new ProtocolConverter(transport, null);

        try {
            converter.loadTranslator("../stomp-test");
            fail("should have failed");
        } catch (InstantiationException e) {
            assertTrue(e.getMessage().contains("rovided key escapes the FactoryFinder configured directory"));
        }

        // fallback
        assertEquals(LegacyFrameTranslator.class, converter.findTranslator("../stomp-test").getClass());
    }

    private void assertTranslatorNotAllowed(ProtocolConverter converter, String key) throws Exception {
        try {
            converter.loadTranslator(key);
            fail("Should have failed");
        } catch (InstantiationException e) {
            assertTrue(e.getMessage().contains("is not an allowed implementation of type interface "
                    + FrameTranslator.class.getName()));
        }
        // should fall back to default legacy on error
        assertEquals(LegacyFrameTranslator.class, converter.findTranslator(key).getClass());
    }

    private static void resetDefaultAllowedTranslators() {
        System.setProperty(ProtocolConverter.FRAME_TRANSLATOR_CLASSES_PROP,
                ProtocolConverter.DEFAULT_ALLOWED_TRANSLATORS);
    }

    // don't implement the correct interface
    public static class BadTranslator {

    }
}
