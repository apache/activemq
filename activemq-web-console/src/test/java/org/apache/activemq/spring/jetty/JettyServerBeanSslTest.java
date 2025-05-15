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
package org.apache.activemq.spring.jetty;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.cert.X509Certificate;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.apache.xbean.spring.context.ClassPathXmlApplicationContext;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.server.SslConnectionFactory;
import org.junit.Test;

/**
 * Validates the console's HTTPS connector: JettyServerBean loads the canonical
 * jetty-ssl.xml / jetty-ssl-context.xml / jetty-https.xml against the sample
 * jetty-keystore.ks keystore, and an actual TLS request to /admin/ is challenged for
 * authentication (proving the TLS handshake and JAAS SecurityHandler both work
 * over HTTPS).
 */
public class JettyServerBeanSslTest {

    @Test
    public void testJettyServerBeanHttps() throws Exception {
        var context = new ClassPathXmlApplicationContext("conf-ssl/jetty-spring.xml");
        context.start();

        try {
            var jettyServer = (JettyServerBean) context.getBean("jettyServer");
            assertNotNull(jettyServer);

            Server server = jettyServer.getServer();
            assertNotNull("Embedded Jetty Server should be created", server);
            assertTrue("Embedded Jetty Server should be running", server.isRunning());

            ServerConnector connector = (ServerConnector) server.getConnectors()[0];
            assertNotNull("The connector must be an HTTPS (TLS) connector",
                    connector.getConnectionFactory(SslConnectionFactory.class));
            int port = connector.getLocalPort();

            // Trust-all SSL context: we are validating the server's TLS + auth wiring, not the
            // (self-signed, dev-only) jetty-keystore.ks certificate. A raw SSLSocket avoids client-side
            // hostname verification against 127.0.0.1.
            SSLContext ssl = SSLContext.getInstance("TLS");
            ssl.init(null, new TrustManager[] { new X509TrustManager() {
                @Override public void checkClientTrusted(X509Certificate[] chain, String authType) { }
                @Override public void checkServerTrusted(X509Certificate[] chain, String authType) { }
                @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
            } }, null);

            SSLSocketFactory factory = ssl.getSocketFactory();
            try (SSLSocket socket = (SSLSocket) factory.createSocket("127.0.0.1", port)) {
                // A successful handshake proves the TLS connector + keystore are wired correctly.
                socket.startHandshake();

                OutputStream out = socket.getOutputStream();
                out.write(("GET /admin/ HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n\r\n")
                        .getBytes(StandardCharsets.US_ASCII));
                out.flush();

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
                String statusLine = reader.readLine();
                assertNotNull("No HTTP response over TLS", statusLine);
                assertTrue("Console over HTTPS must require authentication (401), got: " + statusLine,
                        statusLine.contains("401"));
            }
        } finally {
            context.stop();
        }
    }
}
