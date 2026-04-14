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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.Queue;
import jakarta.jms.QueueBrowser;
import jakarta.jms.Session;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;

import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.web.view.MessageRenderer;
import org.apache.activemq.web.view.RssMessageRenderer;
import org.apache.activemq.web.view.SimpleMessageRenderer;
import org.apache.activemq.web.view.XmlMessageRenderer;

/**
 * Renders the contents of a queue using some kind of view. The URI is assumed
 * to be the queue. The following parameters can be used
 * <p/>
 * <ul>
 * <li>view - specifies the type of the view such as simple, xml, rss</li>
 * <li>selector - specifies the SQL 92 selector to apply to the queue</li>
 * </ul>
 *
 *
 */
public class QueueBrowseServlet extends HttpServlet {

    public static final String QUEUE_BROWSE_VIEWS_PROP = "org.apache.activemq.web.view.QUEUE_BROWSE_CLASSES";
    public static final String DEFAULT_ALLOWED_VIEWS = FactoryFinder.buildAllowedImpls(
            RssMessageRenderer.class, XmlMessageRenderer.class, SimpleMessageRenderer.class);

    private final  FactoryFinder<MessageRenderer> factoryFinder;

    public QueueBrowseServlet() {
        this.factoryFinder = new FactoryFinder<>("META-INF/services/org/apache/activemq/web/view/",
                MessageRenderer.class, System.getProperty(QUEUE_BROWSE_VIEWS_PROP, DEFAULT_ALLOWED_VIEWS));
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        try {
            try(WebClient client = WebClient.getWebClient(request);
                Session session = client.getSession()) {
                Queue queue = getQueue(request, session);
                if (queue == null) {
                    throw new ServletException("No queue URI specified");
                }

                String msgId = request.getParameter("msgId");
                if (msgId == null) {
                    MessageRenderer renderer = getMessageRenderer(request);
                    configureRenderer(request, renderer);

                    String selector = getSelector(request);
                    try (QueueBrowser browser = session.createBrowser(queue, selector)) {
                        renderer.renderMessages(request, response, browser);
                    }
                } else {
                    XmlMessageRenderer renderer = new XmlMessageRenderer();
                    try (QueueBrowser browser = session.createBrowser(queue, "JMSMessageID='" + msgId + "'")) {
                        if (!browser.getEnumeration().hasMoreElements()) {
                            response.sendError(HttpServletResponse.SC_NOT_FOUND);
                            return;
                        }
                        Message message = (Message) browser.getEnumeration().nextElement();

                        PrintWriter writer = response.getWriter();
                        renderer.renderMessage(writer, request, response, browser, message);
                        writer.flush();
                    }
                }
            }
        }
        catch (JMSException e) {
            throw new ServletException(e);
        }
    }

    protected MessageRenderer getMessageRenderer(HttpServletRequest request) throws IOException, ServletException {
        String style = request.getParameter("view");
        if (style == null) {
            style = "simple";
        }
        try {
            return factoryFinder.newInstance(style);
        }
        catch (IllegalAccessException | InstantiationException | ClassNotFoundException e) {
            throw new NoSuchViewStyleException(style, e);
        }
    }

    protected void configureRenderer(HttpServletRequest request, MessageRenderer renderer) {
        Map<String, String> properties = new HashMap<>();
        for (Enumeration<String> iter = request.getParameterNames(); iter.hasMoreElements();) {
            String name = iter.nextElement();
            properties.put(name, request.getParameter(name));
        }
        IntrospectionSupport.setProperties(renderer, properties);
    }

    protected String getSelector(HttpServletRequest request) {
        return request.getParameter("selector");
    }

    protected Queue getQueue(HttpServletRequest request, Session session) throws JMSException {
        String uri = request.getPathInfo();
        if (uri == null) {
            return null;
        }

        // replace URI separator with JMS destination separator
        if (uri.startsWith("/")) {
            uri = uri.substring(1);
            if (uri.length() == 0) {
                return null;
            }
        }
        uri = uri.replace('/', '.');

        return session.createQueue(uri);
    }
}
