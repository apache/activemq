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

import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

/**
 * A simple {@link SslContext} that holds key/trust managers directly and
 * creates a single {@link SSLContext} on first use.
 *
 * <p>This is the recommended replacement for {@link ThreadLocalSslContext}
 * when explicit parameter passing is used throughout the transport chain.
 * It carries no ThreadLocal state and does not support runtime certificate
 * reload.
 */
public class DefaultSslContext extends SslContext {

    private String protocol = "TLS";
    private String provider;
    private KeyManager[] keyManagers;
    private TrustManager[] trustManagers;
    private SecureRandom secureRandom;

    private volatile SSLContext sslContext;

    public DefaultSslContext() {
    }

    public DefaultSslContext(KeyManager[] km, TrustManager[] tm, SecureRandom random) {
        this.keyManagers = km;
        this.trustManagers = tm;
        this.secureRandom = random;
    }

    @Override
    public SSLContext getSSLContext() throws NoSuchProviderException, NoSuchAlgorithmException, KeyManagementException {
        if (sslContext == null) {
            synchronized (this) {
                if (sslContext == null) {
                    SSLContext ctx;
                    if (provider == null) {
                        ctx = SSLContext.getInstance(protocol);
                    } else {
                        ctx = SSLContext.getInstance(protocol, provider);
                    }
                    ctx.init(keyManagers, trustManagers, secureRandom);
                    sslContext = ctx;
                }
            }
        }
        return sslContext;
    }

    // --- Bean properties ---

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public KeyManager[] getKeyManagers() {
        return keyManagers;
    }

    public void setKeyManagers(KeyManager[] keyManagers) {
        this.keyManagers = keyManagers;
    }

    public TrustManager[] getTrustManagers() {
        return trustManagers;
    }

    public void setTrustManagers(TrustManager[] trustManagers) {
        this.trustManagers = trustManagers;
    }

    public SecureRandom getSecureRandom() {
        return secureRandom;
    }

    public void setSecureRandom(SecureRandom secureRandom) {
        this.secureRandom = secureRandom;
    }
}
