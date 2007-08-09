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

import java.io.IOException;
import java.net.URI;

import junit.framework.TestCase;

public class SslTransportServerTest extends TestCase {
    private SslTransportServer sslTransportServer;
    private StubSSLServerSocket sslServerSocket;

    protected void setUp() throws Exception {
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private void createAndBindTransportServer(boolean wantClientAuth, boolean needClientAuth, String options) throws IOException {
        sslServerSocket = new StubSSLServerSocket();

        StubSSLSocketFactory socketFactory = new StubSSLSocketFactory(sslServerSocket);

        try {
            sslTransportServer = new SslTransportServer(null, new URI("ssl://localhost:61616?" + options), socketFactory);
        } catch (Exception e) {
            fail("Unable to create SslTransportServer.");
        }

        sslTransportServer.setWantClientAuth(wantClientAuth);
        sslTransportServer.setNeedClientAuth(needClientAuth);

        sslTransportServer.bind();
    }

    public void testWantAndNeedClientAuthSetters() throws IOException {
        for (int i = 0; i < 4; ++i) {
            final boolean wantClientAuth = ((i & 0x1) == 1);
            final boolean needClientAuth = ((i & 0x2) == 1);

            final int expectedWantStatus = (wantClientAuth ? StubSSLServerSocket.TRUE : StubSSLServerSocket.FALSE);
            final int expectedNeedStatus = (needClientAuth ? StubSSLServerSocket.TRUE : StubSSLServerSocket.FALSE);

            createAndBindTransportServer(wantClientAuth, needClientAuth, "");

            assertEquals("Created ServerSocket did not have correct wantClientAuth status.", sslServerSocket.getWantClientAuthStatus(), expectedWantStatus);

            assertEquals("Created ServerSocket did not have correct needClientAuth status.", sslServerSocket.getNeedClientAuthStatus(), expectedNeedStatus);
        }
    }

    public void testWantAndNeedAuthReflection() throws IOException {
        for (int i = 0; i < 4; ++i) {
            final boolean wantClientAuth = ((i & 0x1) == 1);
            final boolean needClientAuth = ((i & 0x2) == 1);

            final int expectedWantStatus = (wantClientAuth ? StubSSLServerSocket.TRUE : StubSSLServerSocket.FALSE);
            final int expectedNeedStatus = (needClientAuth ? StubSSLServerSocket.TRUE : StubSSLServerSocket.FALSE);

            String options = "wantClientAuth=" + (wantClientAuth ? "true" : "false") + "&needClientAuth=" + (needClientAuth ? "true" : "false");

            createAndBindTransportServer(wantClientAuth, needClientAuth, options);

            assertEquals("Created ServerSocket did not have correct wantClientAuth status.", sslServerSocket.getWantClientAuthStatus(), expectedWantStatus);

            assertEquals("Created ServerSocket did not have correct needClientAuth status.", sslServerSocket.getNeedClientAuthStatus(), expectedNeedStatus);
        }
    }
}
