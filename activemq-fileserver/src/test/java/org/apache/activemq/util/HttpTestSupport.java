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
package org.apache.activemq.util;

import java.io.File;
import java.net.Socket;
import java.net.URL;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.net.SocketFactory;

import junit.framework.TestCase;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerService;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.webapp.WebAppContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class HttpTestSupport extends TestCase {
    private static final Logger LOG = LoggerFactory.getLogger(HttpTestSupport.class);

    BrokerService broker;
    Server server;
    ActiveMQConnectionFactory factory;
    Connection connection;
    Session session;
    MessageProducer producer;
    Destination destination;

    protected boolean createBroker = true;

    final File homeDir = new File("src/main/webapp/uploads/");
    int port;

    private int getPort(Object o) throws Exception {
        return (Integer)o.getClass().getMethod("getLocalPort").invoke(o);
    }
    @Override
    protected void setUp() throws Exception {

        server = new Server(0);
        WebAppContext context = new WebAppContext();

        context.setResourceBase("src/main/webapp");
        context.setContextPath("/");
        context.setServer(server);
        server.setHandler(context);
        server.start();

        port = getPort(server.getConnectors()[0]);

        waitForJettySocketToAccept("http://localhost:" + port);

        if (createBroker) {
            broker = new BrokerService();
            broker.setPersistent(false);
            broker.setUseJmx(true);
            broker.addConnector("vm://localhost");
            broker.start();
            broker.waitUntilStarted();

            factory = new ActiveMQConnectionFactory("vm://localhost");
            factory.getBlobTransferPolicy().setDefaultUploadUrl("http://localhost:" + port + "/uploads/");
            connection = factory.createConnection();
            connection.start();
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            destination = session.createQueue("test");
            producer = session.createProducer(destination);

            IOHelper.deleteFile(homeDir);
            homeDir.mkdir();
        }
    }



    @Override
    protected void tearDown() throws Exception {
        server.stop();
        if (createBroker) {
            session.close();
            connection.close();
            broker.stop();
            broker.waitUntilStopped();
            IOHelper.deleteFile(homeDir);
        }
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

