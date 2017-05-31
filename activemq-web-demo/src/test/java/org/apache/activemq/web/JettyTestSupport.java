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

import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.net.URL;

import javax.jms.Connection;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.net.ServerSocketFactory;
import javax.net.SocketFactory;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.util.Wait;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.webapp.WebAppContext;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JettyTestSupport {
    private static final Logger LOG = LoggerFactory.getLogger(JettyTestSupport.class);

    protected BrokerService broker;
    protected Session session;
    protected MessageProducer producer;
    protected URI tcpUri;
    protected URI stompUri;

    private Server server;
    private ActiveMQConnectionFactory factory;
    private Connection connection;
    private int proxyPort = 0;

    protected boolean isPersistent() {
        return false;
    }

    @Before
    public void setUp() throws Exception {
        broker = new BrokerService();
        broker.setBrokerName("amq-broker");
        broker.setPersistent(isPersistent());
        broker.setDataDirectory("target/activemq-data");
        broker.setUseJmx(true);
        tcpUri = new URI(broker.addConnector("tcp://localhost:0").getPublishableConnectString());
        stompUri = new URI(broker.addConnector("stomp://localhost:0").getPublishableConnectString());
        broker.start();
        broker.waitUntilStarted();

        int port = getPort();
        server = new Server();
        ServerConnector connector = new ServerConnector(server);
        connector.setPort(port);
        WebAppContext context = new WebAppContext();

        context.setResourceBase("src/main/webapp");
        context.setContextPath("/");
        context.setServer(server);
        server.setHandler(context);
        server.setConnectors(new Connector[] {
            connector
        });
        server.start();
        waitForJettySocketToAccept("http://localhost:" + port);

        factory = new ActiveMQConnectionFactory(tcpUri);
        connection = factory.createConnection();
        connection.start();
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        producer = session.createProducer(session.createQueue("test"));
    }


    @After
    public void tearDown() throws Exception {
        session.close();
        connection.close();
        server.stop();
        broker.stop();
        broker.waitUntilStopped();
    }

    protected int getPort() {
        if (proxyPort == 0) {
            ServerSocket ss = null;
            try {
                ss = ServerSocketFactory.getDefault().createServerSocket(0);
                proxyPort = ss.getLocalPort();
            } catch (IOException e) { // ignore
            } finally {
                try {
                    if (ss != null ) {
                        ss.close();
                    }
                } catch (IOException e) { // ignore
                }
            }
        }
        return proxyPort;
    }

    public void waitForJettySocketToAccept(String bindLocation) throws Exception {
        final URL url = new URL(bindLocation);
        assertTrue("Jetty endpoint is available", Wait.waitFor(new Wait.Condition() {

            @Override
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
