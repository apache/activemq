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
package org.apache.activemq.web;

import java.net.Socket;
import java.net.URI;
import java.net.URL;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.net.SocketFactory;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.webapp.WebAppContext;

public class JettyTestSupport extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(JettyTestSupport.class);

    BrokerService broker;
    Server server;
    ActiveMQConnectionFactory factory;
    Connection connection;
    Session session;
    MessageProducer producer;

    URI tcpUri;
    URI stompUri;

    protected void setUp() throws Exception {
        broker = new BrokerService();
        broker.setPersistent(false);
        broker.setUseJmx(true);
        tcpUri = broker.addConnector("tcp://localhost:61616").getConnectUri();
        stompUri = broker.addConnector("stomp://localhost:61613").getConnectUri();
        broker.start();
        broker.waitUntilStarted();

        server = new Server();
        SelectChannelConnector connector = new SelectChannelConnector();
        connector.setPort(8080);
        connector.setServer(server);
        WebAppContext context = new WebAppContext();

        context.setResourceBase("src/main/webapp");
        context.setContextPath("/");
        context.setServer(server);
        server.setHandler(context);
        server.setConnectors(new Connector[] {
            connector
        });
        server.start();
        waitForJettySocketToAccept("http://localhost:8080");

        factory = new ActiveMQConnectionFactory(tcpUri);
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(session.createQueue("test"));
    }

    protected void tearDown() throws Exception {
        server.stop();
        broker.stop();
        broker.waitUntilStopped();
        session.close();
        connection.close();
    }

    public void waitForJettySocketToAccept(String bindLocation) throws Exception {
        final URL url = new URL(bindLocation);
        assertTrue("Jetty endpoint is available", Wait.waitFor(new Wait.Condition() {

            public boolean isSatisified() throws Exception {
                boolean canConnect = false;
                try {
                    Socket socket = SocketFactory.getDefault().createSocket(url.getHost(), url.getPort());
                    socket.close();
                    canConnect = true;
                } catch (Exception e) {
                    LOG.warn("verify jetty available, failed to connect to " + url + e);
                }
                return canConnect;
            }}, 60 * 1000));
    }

}
