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
import java.security.cert.X509Certificate;

import javax.management.remote.JMXPrincipal;
import javax.net.ssl.SSLSocket;

/**
 * 
 */
public final class SslSocketHelper {

    private SslSocketHelper() {
    }

    public static SSLSocket createSSLSocket(String certDistinguishedName, boolean wantAuth, boolean needAuth)
        throws IOException {
        JMXPrincipal principal = new JMXPrincipal(certDistinguishedName);
        X509Certificate cert = new StubX509Certificate(principal);
        StubSSLSession sslSession = new StubSSLSession(cert);

        StubSSLSocket sslSocket = new StubSSLSocket(sslSession);
        sslSocket.setWantClientAuth(wantAuth);
        sslSocket.setNeedClientAuth(needAuth);
        return sslSocket;
    }
}
