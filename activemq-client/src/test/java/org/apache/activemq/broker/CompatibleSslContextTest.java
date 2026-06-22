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
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import org.junit.Test;

public class CompatibleSslContextTest {

    @Test
    public void testGetSSLContextReturnsNonNull() throws Exception {
        CompatibleSslContext ctx = new CompatibleSslContext();
        SSLContext sslCtx = ctx.getSSLContext();
        assertNotNull(sslCtx);
        assertEquals("TLS", sslCtx.getProtocol());
    }

    @Test
    public void testGetSSLContextIsSingleton() throws Exception {
        CompatibleSslContext ctx = new CompatibleSslContext();
        SSLContext first = ctx.getSSLContext();
        SSLContext second = ctx.getSSLContext();
        assertSame(first, second);
    }

    @Test
    public void testArrayConstructor() throws Exception {
        TrustManager tm = new PermissiveTrustManager();
        CompatibleSslContext ctx = new CompatibleSslContext(null, new TrustManager[]{tm}, null);

        List<TrustManager> list = ctx.getTrustManagers();
        assertEquals(1, list.size());
        assertSame(tm, list.get(0));
        assertTrue(ctx.getKeyManagers().isEmpty());
    }

    @Test
    public void testListBasedGettersAndSetters() {
        CompatibleSslContext ctx = new CompatibleSslContext();

        assertTrue(ctx.getKeyManagers().isEmpty());
        assertTrue(ctx.getTrustManagers().isEmpty());

        TrustManager tm = new PermissiveTrustManager();
        ctx.setTrustManagers(Arrays.asList(tm));
        assertEquals(1, ctx.getTrustManagers().size());

        TrustManager[] array = ctx.getTrustManagersAsArray();
        assertEquals(1, array.length);
        assertSame(tm, array[0]);
    }

    @Test
    public void testAddRemoveKeyManager() {
        CompatibleSslContext ctx = new CompatibleSslContext();
        KeyManager km = new DummyKeyManager();

        ctx.addKeyManager(km);
        assertEquals(1, ctx.getKeyManagers().size());
        assertEquals(1, ctx.getKeyManagersAsArray().length);

        assertTrue(ctx.removeKeyManager(km));
        assertTrue(ctx.getKeyManagers().isEmpty());
    }

    @Test
    public void testAddRemoveTrustManager() {
        CompatibleSslContext ctx = new CompatibleSslContext();
        TrustManager tm = new PermissiveTrustManager();

        ctx.addTrustManager(tm);
        assertEquals(1, ctx.getTrustManagers().size());
        assertEquals(1, ctx.getTrustManagersAsArray().length);

        assertTrue(ctx.removeTrustManager(tm));
        assertTrue(ctx.getTrustManagers().isEmpty());
    }

    @Test
    public void testProtocolOverride() throws Exception {
        CompatibleSslContext ctx = new CompatibleSslContext();
        ctx.setProtocol("TLSv1.2");
        assertEquals("TLSv1.2", ctx.getProtocol());

        SSLContext sslCtx = ctx.getSSLContext();
        assertEquals("TLSv1.2", sslCtx.getProtocol());
    }

    @Test
    public void testSetSSLContextDirectly() throws Exception {
        CompatibleSslContext ctx = new CompatibleSslContext();
        SSLContext manual = SSLContext.getInstance("TLS");
        manual.init(null, null, null);
        ctx.setSSLContext(manual);

        assertSame(manual, ctx.getSSLContext());
    }

    @Test
    public void testBeanProperties() {
        CompatibleSslContext ctx = new CompatibleSslContext();

        assertEquals("TLS", ctx.getProtocol());
        assertNull(ctx.getProvider());
        assertNull(ctx.getSecureRandom());

        ctx.setProvider("SunJSSE");
        assertEquals("SunJSSE", ctx.getProvider());

        SecureRandom sr = new SecureRandom();
        ctx.setSecureRandom(sr);
        assertSame(sr, ctx.getSecureRandom());
    }

    @Test
    public void testNoThreadLocalSideEffects() throws Exception {
        SslContext before = SslContext.getCurrentSslContext();
        CompatibleSslContext ctx = new CompatibleSslContext();
        ctx.getSSLContext();
        assertSame("CompatibleSslContext must not touch ThreadLocal",
                before, SslContext.getCurrentSslContext());
    }

    @Test
    public void testProtectedFieldAccessFromSubclass() throws Exception {
        CompatibleSslContext ctx = new CompatibleSslContext();

        ctx.keyManagers.add(new DummyKeyManager());
        ctx.trustManagers.add(new PermissiveTrustManager());
        ctx.secureRandom = new SecureRandom();

        assertEquals(1, ctx.getKeyManagersAsArray().length);
        assertEquals(1, ctx.getTrustManagersAsArray().length);
        assertNotNull(ctx.getSecureRandom());
    }

    @Test
    public void testReloadIsNoOp() throws Exception {
        CompatibleSslContext ctx = new CompatibleSslContext();
        SSLContext before = ctx.getSSLContext();
        ctx.reload();
        assertSame(before, ctx.getSSLContext());
    }

    @Test
    public void testApiCompatibilityWithThreadLocalSslContext() throws Exception {
        ThreadLocalSslContext threadLocal = new ThreadLocalSslContext();
        CompatibleSslContext compatible = new CompatibleSslContext();

        threadLocal.setProtocol("TLSv1.2");
        compatible.setProtocol("TLSv1.2");

        TrustManager tm = new PermissiveTrustManager();
        threadLocal.addTrustManager(tm);
        compatible.addTrustManager(tm);

        assertEquals(threadLocal.getTrustManagers().size(), compatible.getTrustManagers().size());
        assertEquals(threadLocal.getTrustManagersAsArray().length, compatible.getTrustManagersAsArray().length);
        assertEquals(threadLocal.getProtocol(), compatible.getProtocol());
        assertEquals(threadLocal.getSSLContext().getProtocol(), compatible.getSSLContext().getProtocol());
    }

    private static class PermissiveTrustManager implements X509TrustManager {
        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) {}
        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) {}
        @Override
        public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }
    }

    private static class DummyKeyManager implements KeyManager {
    }
}
