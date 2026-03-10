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
package org.apache.activemq.web;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.QueueBrowser;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.PrintWriter;
import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.web.view.MessageRenderer;
import org.apache.activemq.web.view.RssMessageRenderer;
import org.apache.activemq.web.view.SimpleMessageRenderer;
import org.apache.activemq.web.view.XmlMessageRenderer;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class QueueBrowseServletTest {

    private final HttpServletRequest request = mock(HttpServletRequest.class);

    // Before each test reset to defaults
    @Before
    public void setUp() {
        resetDefaultAllowedViews();
    }

    // clean up after all tests are done finished by resetting the property back before exiting
    @AfterClass
    public static void tearDown() {
        resetDefaultAllowedViews();
    }

    @Test
    public void testDefaultViews() throws Exception {
        QueueBrowseServlet servlet = new QueueBrowseServlet();

        when(request.getParameter("view")).thenReturn("simple");
        assertEquals(SimpleMessageRenderer.class, servlet.getMessageRenderer(request).getClass());

        when(request.getParameter("view")).thenReturn(null);
        assertEquals(SimpleMessageRenderer.class, servlet.getMessageRenderer(request).getClass());

        when(request.getParameter("view")).thenReturn("xml");
        assertEquals(XmlMessageRenderer.class, servlet.getMessageRenderer(request).getClass());

        when(request.getParameter("view")).thenReturn("rss");
        assertEquals(RssMessageRenderer.class, servlet.getMessageRenderer(request).getClass());
    }

    @Test
    public void testPathTraversal() throws Exception {
        QueueBrowseServlet servlet = new QueueBrowseServlet();
        // illegal path traversal
        when(request.getParameter("view")).thenReturn("../../simple");
        try {
            servlet.getMessageRenderer(request);
            fail("Should have thrown an exception");
        } catch (NoSuchViewStyleException e) {
            Throwable rootCause = e.getRootCause();
            assertTrue(rootCause instanceof InstantiationException);
            // view is in allow list but wrong interface type
            assertEquals(rootCause.getMessage(), "Provided key escapes the FactoryFinder configured directory");
        }
    }

    @Test
    public void testViewAllowlistSimpleOnly() throws Exception {
        // only allow simple and rss view
        System.setProperty(QueueBrowseServlet.QUEUE_BROWSE_VIEWS_PROP,
                FactoryFinder.buildAllowedImpls(SimpleMessageRenderer.class,
                        RssMessageRenderer.class));

        QueueBrowseServlet servlet = new QueueBrowseServlet();

        // simple/rss are allowed
        when(request.getParameter("view")).thenReturn("simple");
        assertEquals(SimpleMessageRenderer.class, servlet.getMessageRenderer(request).getClass());
        when(request.getParameter("view")).thenReturn("rss");
        assertEquals(RssMessageRenderer.class, servlet.getMessageRenderer(request).getClass());

        // XmlMessageRenderer is not in the allow list so this should be blocked
        assertViewAllowListFailure(servlet, "xml");
    }

    @Test
    public void testViewAllowlistNone() throws Exception {
        // Set to none allowed
        System.setProperty(QueueBrowseServlet.QUEUE_BROWSE_VIEWS_PROP, "");
        QueueBrowseServlet servlet = new QueueBrowseServlet();
        assertViewAllowListFailure(servlet, "simple");
        assertViewAllowListFailure(servlet, "xml");
    }

    @Test
    public void testViewAllowAll() throws Exception {
        // Allow all
        System.setProperty(QueueBrowseServlet.QUEUE_BROWSE_VIEWS_PROP, "*");
        QueueBrowseServlet servlet = new QueueBrowseServlet();

        when(request.getParameter("view")).thenReturn("simple");
        assertEquals(SimpleMessageRenderer.class, servlet.getMessageRenderer(request).getClass());
    }

    @Test
    public void testMissingInterface() throws Exception {
        // Add class with wrong interface to allow list
        System.setProperty(QueueBrowseServlet.QUEUE_BROWSE_VIEWS_PROP, InvalidMessageRender.class.getName());

        try {
            // servlet should fail on creation
            new QueueBrowseServlet();
            fail("Should have thrown an exception");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains(InvalidMessageRender.class.getName() +
                    " is not assignable to interface " + MessageRenderer.class.getName()));
        }
        when(request.getParameter("view")).thenReturn("invalid-view");

        // set to allow all
        System.setProperty(QueueBrowseServlet.QUEUE_BROWSE_VIEWS_PROP, "*");
        QueueBrowseServlet servlet = new QueueBrowseServlet();

        // should fail on lookup with same interface issue
        try {
            when(request.getParameter("view")).thenReturn("invalid-view");
            servlet.getMessageRenderer(request);
            fail("Should not be allowed");
        } catch (NoSuchViewStyleException e) {
            Throwable rootCause = e.getRootCause();
            assertTrue(rootCause instanceof InstantiationException);
            // view is in allow list but wrong interface type
            assertTrue(rootCause.getMessage().contains(InvalidMessageRender.class.getName() +
                    " is not assignable to interface " + MessageRenderer.class.getName()));
        }
    }

    @Test
    public void testViewCustomAllow() throws Exception {
        // default view settings
        QueueBrowseServlet servlet = new QueueBrowseServlet();
        when(request.getParameter("view")).thenReturn("valid-view");

        // custom view is not in the allow list
        assertViewAllowListFailure(servlet, "valid-view");

        // reset with view added to allow list
        System.setProperty(QueueBrowseServlet.QUEUE_BROWSE_VIEWS_PROP, ValidMessageRender.class.getName());
        servlet = new QueueBrowseServlet();

        assertEquals(ValidMessageRender.class, servlet.getMessageRenderer(request).getClass());
    }

    @Test
    public void testViewsWhiteSpace() throws Exception {
        // add views with extra white space which should get trimmed
        System.setProperty(QueueBrowseServlet.QUEUE_BROWSE_VIEWS_PROP,
                String.join(",  ", SimpleMessageRenderer.class.getName(),
                        RssMessageRenderer.class.getName()));

        QueueBrowseServlet servlet = new QueueBrowseServlet();
        when(request.getParameter("view")).thenReturn("simple");
        assertEquals(SimpleMessageRenderer.class, servlet.getMessageRenderer(request).getClass());
        when(request.getParameter("view")).thenReturn("rss");
        assertEquals(RssMessageRenderer.class, servlet.getMessageRenderer(request).getClass());
    }

    private void assertViewAllowListFailure(QueueBrowseServlet servlet, String view) throws Exception {
        try {
            when(request.getParameter("view")).thenReturn(view);
            servlet.getMessageRenderer(request);
            fail("Should not be allowed");
        } catch (NoSuchViewStyleException e) {
            Throwable rootCause = e.getRootCause();
            assertTrue(rootCause instanceof InstantiationException);
            assertTrue(rootCause.getMessage().contains("is not an allowed "
                    + "implementation of type interface " + MessageRenderer.class.getName()));
        }
    }

    private static void resetDefaultAllowedViews() {
        System.setProperty(QueueBrowseServlet.QUEUE_BROWSE_VIEWS_PROP,
                QueueBrowseServlet.DEFAULT_ALLOWED_VIEWS);
    }

    // Does not implement the right interface
    public static class InvalidMessageRender {

    }

    public static class ValidMessageRender implements MessageRenderer {

        @Override
        public void renderMessages(HttpServletRequest request, HttpServletResponse response,
                QueueBrowser browser) throws IOException, JMSException, ServletException {

        }

        @Override
        public void renderMessage(PrintWriter writer, HttpServletRequest request,
                HttpServletResponse response, QueueBrowser browser, Message message)
                throws JMSException, ServletException {

        }
    }
}
