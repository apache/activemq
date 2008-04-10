/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.transport.tcp;

import junit.framework.TestCase;

import java.io.IOException;
import java.net.URI;

public class SslTransportServerTest extends TestCase {
    private SslTransportServer sslTransportServer = null;
    private StubSSLServerSocket sslServerSocket = null;
    
    protected void setUp() throws Exception {
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }
    
    private void createAndBindTransportServer(boolean wantClientAuth, boolean needClientAuth, String options)
            throws IOException {
        sslServerSocket = new StubSSLServerSocket();
        
        StubSSLSocketFactory socketFactory = new StubSSLSocketFactory(sslServerSocket);
        
        try {
            sslTransportServer =
                new SslTransportServer(null, new URI("ssl://localhost:61616?" + options), socketFactory);
        } catch (Exception e) {
            fail("Unable to create SslTransportServer.");
        }
        
        sslTransportServer.setWantClientAuth(wantClientAuth);
        sslTransportServer.setNeedClientAuth(needClientAuth);
        
        sslTransportServer.bind();
    }
    
    public void testWantAndNeedClientAuthSetters() throws IOException {
        for (int i = 0; i < 4; ++i) {
            String options = "";
            singleTest(i, options);
            }
    }
    
    public void testWantAndNeedAuthReflection() throws IOException {
        for (int i = 0; i < 4; ++i) {
            String options = "wantClientAuth=" + (getWantClientAuth(i) ? "true" : "false") +
                "&needClientAuth=" + (getNeedClientAuth(i) ? "true" : "false");
            singleTest(i, options);
        }
    }

    private void singleTest(int i, String options) throws IOException {
        final boolean wantClientAuth = getWantClientAuth(i);
        final boolean needClientAuth = getNeedClientAuth(i);

        final int expectedWantStatus = (needClientAuth? StubSSLServerSocket.UNTOUCHED: wantClientAuth ? StubSSLServerSocket.TRUE : StubSSLServerSocket.UNTOUCHED);
        final int expectedNeedStatus = (needClientAuth ? StubSSLServerSocket.TRUE : StubSSLServerSocket.UNTOUCHED );


        createAndBindTransportServer(wantClientAuth, needClientAuth, options);

        assertEquals("Created ServerSocket did not have correct wantClientAuth status. wantClientAuth: " + wantClientAuth + ", needClientAuth: " + needClientAuth,
            expectedWantStatus, sslServerSocket.getWantClientAuthStatus());

        assertEquals("Created ServerSocket did not have correct needClientAuth status. wantClientAuth: " + wantClientAuth + ", needClientAuth: " + needClientAuth,
            expectedNeedStatus, sslServerSocket.getNeedClientAuthStatus());
    }

    private boolean getNeedClientAuth(int i) {
        return ((i & 0x2) == 0x2);
    }

    private boolean getWantClientAuth(int i) {
        return ((i & 0x1) == 0x1);
    }
}
