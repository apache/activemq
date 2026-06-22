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
package org.apache.activemq.broker;

import static org.junit.Assert.*;

import java.security.SecureRandom;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.security.cert.X509Certificate;

import org.junit.Test;

public class DefaultSslContextTest {

    @Test
    public void testGetSSLContextReturnsNonNull() throws Exception {
        DefaultSslContext ctx = new DefaultSslContext();
        SSLContext sslCtx = ctx.getSSLContext();
        assertNotNull(sslCtx);
        assertEquals("TLS", sslCtx.getProtocol());
    }

    @Test
    public void testGetSSLContextIsSingleton() throws Exception {
        DefaultSslContext ctx = new DefaultSslContext();
        SSLContext first = ctx.getSSLContext();
        SSLContext second = ctx.getSSLContext();
        assertSame(first, second);
    }

    @Test
    public void testConstructorWithManagers() throws Exception {
        TrustManager tm = new PermissiveTrustManager();
        DefaultSslContext ctx = new DefaultSslContext(null, new TrustManager[]{tm}, null);

        SSLContext sslCtx = ctx.getSSLContext();
        assertNotNull(sslCtx);
        assertArrayEquals(new TrustManager[]{tm}, ctx.getTrustManagers());
    }

    @Test
    public void testProtocolOverride() throws Exception {
        DefaultSslContext ctx = new DefaultSslContext();
        ctx.setProtocol("TLSv1.2");
        assertEquals("TLSv1.2", ctx.getProtocol());

        SSLContext sslCtx = ctx.getSSLContext();
        assertEquals("TLSv1.2", sslCtx.getProtocol());
    }

    @Test
    public void testReloadIsNoOp() throws Exception {
        DefaultSslContext ctx = new DefaultSslContext();
        SSLContext before = ctx.getSSLContext();
        ctx.reload();
        assertSame(before, ctx.getSSLContext());
    }

    @Test
    public void testBeanProperties() {
        DefaultSslContext ctx = new DefaultSslContext();

        assertNull(ctx.getKeyManagers());
        assertNull(ctx.getTrustManagers());
        assertNull(ctx.getSecureRandom());
        assertNull(ctx.getProvider());
        assertEquals("TLS", ctx.getProtocol());

        KeyManager[] kms = new KeyManager[0];
        ctx.setKeyManagers(kms);
        assertSame(kms, ctx.getKeyManagers());

        TrustManager[] tms = new TrustManager[0];
        ctx.setTrustManagers(tms);
        assertSame(tms, ctx.getTrustManagers());

        SecureRandom sr = new SecureRandom();
        ctx.setSecureRandom(sr);
        assertSame(sr, ctx.getSecureRandom());

        ctx.setProvider("SunJSSE");
        assertEquals("SunJSSE", ctx.getProvider());
    }

    @Test
    public void testNoThreadLocalSideEffects() throws Exception {
        SslContext before = SslContext.getCurrentSslContext();
        DefaultSslContext ctx = new DefaultSslContext();
        ctx.getSSLContext();
        assertSame("DefaultSslContext must not touch ThreadLocal",
                before, SslContext.getCurrentSslContext());
    }

    private static class PermissiveTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}
        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}
        @Override
        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
    }
}
