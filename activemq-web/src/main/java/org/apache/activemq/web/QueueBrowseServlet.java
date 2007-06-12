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
import java.util.Enumeration;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.QueueBrowser;
import javax.jms.Session;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.activemq.util.FactoryFinder;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.web.view.MessageRenderer;
import org.apache.activemq.ActiveMQConnectionFactory;

/**
 * Renders the contents of a queue using some kind of view. The URI is assumed
 * to be the queue. The following parameters can be used
 * 
 * <ul>
 * <li>view - specifies the type of the view such as simple, xml, rss</li>
 * <li>selector - specifies the SQL 92 selector to apply to the queue</li>
 * </ul>
 * 
 * @version $Revision: $
 */
//TODO Why do we implement our own session pool?
//TODO This doesn't work, since nobody will be setting the connection factory (because nobody is able to). Just use the WebClient?
public class QueueBrowseServlet extends HttpServlet {

    private static FactoryFinder factoryFinder = new FactoryFinder("META-INF/services/org/apache/activemq/web/view/");

    private ConnectionFactory connectionFactory;
    private Connection connection;
    private LinkedList sessions = new LinkedList();

    public Connection getConnection() throws JMSException {
        if (connection == null) {
            connection = getConnectionFactory().createConnection();
            connection.start();
        }
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public ConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            String uri = getServletContext().getInitParameter("org.apache.activemq.brokerURL");
            if (uri != null) {
                connectionFactory = new ActiveMQConnectionFactory(uri);
            }
            else {
                throw new IllegalStateException("missing ConnectionFactory in QueueBrowserServlet");
            }
        }
        return connectionFactory;
    }

    public void setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
        Session session = null;
        try {
            session = borrowSession();
            Queue queue = getQueue(request, session);
            if (queue == null) {
                throw new ServletException("No queue URI specified");
            }
            String selector = getSelector(request);
            QueueBrowser browser = session.createBrowser(queue, selector);
            MessageRenderer renderer = getMessageRenderer(request);
            configureRenderer(request, renderer);
            renderer.renderMessages(request, response, browser);
        }
        catch (JMSException e) {
            throw new ServletException(e);
        }
        finally {
            returnSession(session);
        }
    }

    protected MessageRenderer getMessageRenderer(HttpServletRequest request) throws IOException, ServletException {
        String style = request.getParameter("view");
        if (style == null) {
            style = "simple";
        }
        try {
            return (MessageRenderer) factoryFinder.newInstance(style);
        }
        catch (IllegalAccessException e) {
            throw new NoSuchViewStyleException(style, e);
        }
        catch (InstantiationException e) {
            throw new NoSuchViewStyleException(style, e);
        }
        catch (ClassNotFoundException e) {
            throw new NoSuchViewStyleException(style, e);
        }
    }

    protected void configureRenderer(HttpServletRequest request, MessageRenderer renderer) {
        Map properties = new HashMap();
        for (Enumeration iter = request.getParameterNames(); iter.hasMoreElements(); ) {
            String name = (String) iter.nextElement();
            properties.put(name, request.getParameter(name));
        }
        IntrospectionSupport.setProperties(renderer, properties);
    }

    protected Session borrowSession() throws JMSException {
        Session answer = null;
        synchronized (sessions) {
            if (sessions.isEmpty()) {
                answer = createSession();
            }
            else {
                answer = (Session) sessions.removeLast();
            }
        }
        return answer;
    }

    protected void returnSession(Session session) {
        if (session != null) {
            synchronized (sessions) {
                sessions.add(session);
            }
        }
    }

    protected Session createSession() throws JMSException {
        return getConnection().createSession(false, Session.AUTO_ACKNOWLEDGE);
    }

    protected String getSelector(HttpServletRequest request) {
        return request.getParameter("selector");
    }

    protected Queue getQueue(HttpServletRequest request, Session session) throws JMSException {
        String uri = request.getPathInfo();
        if (uri == null)
            return null;

        // replace URI separator with JMS destination separator
        if (uri.startsWith("/")) {
            uri = uri.substring(1);
            if (uri.length() == 0)
                return null;
        }
        uri = uri.replace('/', '.');

        System.out.println("destination uri = " + uri);

        return session.createQueue(uri);
    }

}
