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
package org.apache.activemq.transport.tcp;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnection;
import org.apache.activemq.broker.TransportConnector;

import org.apache.activemq.store.kahadb.MessageDatabase;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.filter.AbstractFilter;
import org.apache.logging.log4j.core.layout.MessageLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.net.Socket;
import java.net.URI;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TcpTransportCloseSocketNoWarnTest {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TcpTransportCloseSocketNoWarnTest.class);

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";
    public static final Appender appender;
    public static final AtomicBoolean gotExceptionInLog = new AtomicBoolean();

    private BrokerService brokerService;
    private Logger rootLogger;

    static {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
        
        appender = new AbstractAppender("testAppender", new AbstractFilter() {}, new MessageLayout(), false, new Property[0]) {
            @Override
            public void append(LogEvent event) {
                if (event.getLevel().equals(Level.WARN) && event.getMessage().getFormattedMessage().contains("failed:")) {
                    gotExceptionInLog.set(Boolean.TRUE);
                    LOG.error("got event: " + event + ", ex:" + event.getMessage().getFormattedMessage());
                    LOG.error("Event source: ", new Throwable("Here"));
                }
                return;
            }
        };
        appender.start();
    }

    @Before
    public void before() throws Exception {
        gotExceptionInLog.set(false);
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);

        rootLogger = org.apache.logging.log4j.core.Logger.class.cast(LogManager.getRootLogger());
        rootLogger.get().addAppender(appender, Level.DEBUG, new AbstractFilter() {});
        rootLogger.addAppender(appender);

        Configurator.setLevel(TransportConnection.class.getName() + ".Transport", Level.WARN);
    }

    @After
    public void after() throws Exception {
        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
        rootLogger.removeAppender(appender);
    }

    @Test(timeout = 60000)
    public void testNoWarn() throws Exception {
        doTest(false);
    }

    @Test(timeout = 60000)
    public void testWarn() throws Exception {
        doTest(true);
    }

    protected void doTest(boolean warn) throws Exception {
        for (String protocol : new String[] {"tcp", "ssl", "stomp"}) {
            TransportConnector transportConnector = brokerService.addConnector(protocol + "://localhost:0");
            transportConnector.setWarnOnRemoteClose(warn);
        }
        this.brokerService = brokerService;
        brokerService.start();
        brokerService.waitUntilStarted();

        for (TransportConnector transportConnector : brokerService.getTransportConnectors()) {
            URI uri = transportConnector.getPublishableConnectURI();
            Socket socket;
            if (uri.getScheme().equals("ssl")) {
                SSLSocket sslSocket = (SSLSocket) SSLSocketFactory.getDefault().createSocket("127.0.0.1", uri.getPort());
                final CountDownLatch doneHandShake = new CountDownLatch(1);
                sslSocket.addHandshakeCompletedListener(new HandshakeCompletedListener() {
                    @Override
                    public void handshakeCompleted(HandshakeCompletedEvent handshakeCompletedEvent) {
                        doneHandShake.countDown();
                    }
                });
                sslSocket.startHandshake();
                assertTrue("handshake done", doneHandShake.await(10, TimeUnit.SECONDS));

                socket = sslSocket;
            } else {
                socket = new Socket("127.0.0.1", uri.getPort());
            }
            // ensure broker gets a chance to send on the new connection
            TimeUnit.SECONDS.sleep(1);
            LOG.info("testing socket: " + socket);
            socket.close();
        }
        assertEquals("warn|no warn in log", warn, gotExceptionInLog.get());
    }
}
