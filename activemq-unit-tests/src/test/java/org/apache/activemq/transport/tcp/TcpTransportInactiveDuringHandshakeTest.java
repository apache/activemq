/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.transport.tcp;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.util.DefaultTestAppender;
import org.apache.activemq.util.Wait;
import org.apache.log4j.Level;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertTrue;

public class TcpTransportInactiveDuringHandshakeTest {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(TcpTransportInactiveDuringHandshakeTest.class);

    public static final String KEYSTORE_TYPE = "jks";
    public static final String PASSWORD = "password";
    public static final String SERVER_KEYSTORE = "src/test/resources/server.keystore";
    public static final String TRUST_KEYSTORE = "src/test/resources/client.keystore";

    static {
        System.setProperty("javax.net.ssl.trustStore", TRUST_KEYSTORE);
        System.setProperty("javax.net.ssl.trustStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.trustStoreType", KEYSTORE_TYPE);
        System.setProperty("javax.net.ssl.keyStore", SERVER_KEYSTORE);
        System.setProperty("javax.net.ssl.keyStorePassword", PASSWORD);
        System.setProperty("javax.net.ssl.keyStoreType", KEYSTORE_TYPE);
    }

    private BrokerService brokerService;
    private DefaultTestAppender appender;
    CountDownLatch inactivityMonitorFired;
    CountDownLatch handShakeComplete;

    @Before
    public void before() throws Exception {
        brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);

        inactivityMonitorFired = new CountDownLatch(1);
        handShakeComplete = new CountDownLatch(1);
        appender = new DefaultTestAppender() {
            @Override
            public void doAppend(LoggingEvent event) {
                if (event.getLevel().equals(Level.WARN) && event.getRenderedMessage().contains("InactivityIOException")) {
                    inactivityMonitorFired.countDown();
                }
            }
        };
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        rootLogger.addAppender(appender);

    }

    @After
    public void after() throws Exception {
        org.apache.log4j.Logger rootLogger = org.apache.log4j.Logger.getRootLogger();
        rootLogger.removeAppender(appender);

        if (brokerService != null) {
            brokerService.stop();
            brokerService.waitUntilStopped();
        }
    }

    @Test
    public void testInactivityMonitorThreadCompletesWhenFiringDuringStart() throws Exception {
        brokerService.addConnector("mqtt+nio+ssl://localhost:0?transport.connectAttemptTimeout=1000&transport.closeAsync=false");
        brokerService.start();
        brokerService.waitUntilStarted();

        TransportConnector transportConnector = brokerService.getTransportConnectors().get(0);
        URI uri = transportConnector.getPublishableConnectURI();


        final CountDownLatch blockHandShakeCompletion = new CountDownLatch(1);

        TrustManager[] trustManagers = new TrustManager[]{new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
                LOG.info("Check Server Trusted: " + s, new Throwable("HERE"));
                try {
                    blockHandShakeCompletion.await(20, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                LOG.info("Check Server Trusted done!");
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        }};


        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(null, trustManagers, new SecureRandom());

        final SSLSocket sslSocket = (SSLSocket) sslContext.getSocketFactory().createSocket("127.0.0.1", uri.getPort());

        sslSocket.addHandshakeCompletedListener(new HandshakeCompletedListener() {
            @Override
            public void handshakeCompleted(HandshakeCompletedEvent handshakeCompletedEvent) {
                handShakeComplete.countDown();
            }
        });

        Executors.newCachedThreadPool().submit(new Runnable() {
            @Override
            public void run() {
                try {
                    sslSocket.startHandshake();
                    assertTrue("Socket connected", sslSocket.isConnected());
                } catch (IOException oops) {
                    oops.printStackTrace();
                }

            }
        });

        assertTrue("inactivity fired", inactivityMonitorFired.await(10, TimeUnit.SECONDS));

        assertTrue("Found non blocked inactivity monitor thread - done its work", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                // verify no InactivityMonitor Task blocked
                Thread[] threads = new Thread[20];
                int activeCount = Thread.currentThread().getThreadGroup().enumerate(threads);
                for (int i = 0; i<activeCount; i++) {
                    Thread thread = threads[i];
                    LOG.info("T[" + i + "]: " + thread);
                    if (thread.getName().contains("InactivityMonitor") && thread.getState().equals(Thread.State.TIMED_WAITING)) {
                        LOG.info("Found inactivity monitor in timed-wait");
                        // good
                        return true;
                    }
                }
                return false;
            }
        }));

        // allow handshake to complete
        blockHandShakeCompletion.countDown();

        final OutputStream socketOutPutStream = sslSocket.getOutputStream();

        assertTrue("Handshake complete", handShakeComplete.await(10, TimeUnit.SECONDS));

        // wait for socket to be closed via Inactivity monitor

        assertTrue("socket error", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                LOG.info("Expecting socket to error from remote close: " + sslSocket);
                try {
                    socketOutPutStream.write(2);
                    socketOutPutStream.flush();
                } catch (IOException expected) {
                    return true;
                }
                return false;
            }
        }));

        LOG.info("Socket at end: " + sslSocket);
        sslSocket.close();
    }
}
