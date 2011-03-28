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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;

/**
 * A holder of SSL configuration.
 */
public class SslContext {
    
    protected String protocol = "TLS";
    protected String provider = null;
    protected List<KeyManager> keyManagers = new ArrayList<KeyManager>();
    protected List<TrustManager> trustManagers = new ArrayList<TrustManager>();
    protected SecureRandom secureRandom;
    private SSLContext sslContext;
    
    private static final ThreadLocal<SslContext> current = new ThreadLocal<SslContext>();
    
    public SslContext() {
    }
    
    public SslContext(KeyManager[] km, TrustManager[] tm, SecureRandom random) {
        if( km!=null ) {
            setKeyManagers(Arrays.asList(km));
        }
        if( tm!=null ) {
            setTrustManagers(Arrays.asList(tm));
        }
        setSecureRandom(random);        
    }
    
    static public void setCurrentSslContext(SslContext bs) {
        current.set(bs);
    }
    static public SslContext getCurrentSslContext() {
        return current.get();
    }
    
    public KeyManager[] getKeyManagersAsArray() {
        KeyManager rc[] = new KeyManager[keyManagers.size()];
        return keyManagers.toArray(rc);
    }
    public TrustManager[] getTrustManagersAsArray() {
        TrustManager rc[] = new TrustManager[trustManagers.size()];
        return trustManagers.toArray(rc);
    }
    
    public void addKeyManager(KeyManager km) {
        keyManagers.add(km);
    }
    public boolean removeKeyManager(KeyManager km) {
        return keyManagers.remove(km);
    }
    public void addTrustManager(TrustManager tm) {
        trustManagers.add(tm);
    }
    public boolean removeTrustManager(TrustManager tm) {
        return trustManagers.remove(tm);
    }
    
    public List<KeyManager> getKeyManagers() {
        return keyManagers;
    }
    public void setKeyManagers(List<KeyManager> keyManagers) {
        this.keyManagers = keyManagers;
    }
    public List<TrustManager> getTrustManagers() {
        return trustManagers;
    }
    public void setTrustManagers(List<TrustManager> trustManagers) {
        this.trustManagers = trustManagers;
    }
    public SecureRandom getSecureRandom() {
        return secureRandom;
    }
    public void setSecureRandom(SecureRandom secureRandom) {
        this.secureRandom = secureRandom;
    }
        
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

    public SSLContext getSSLContext() throws NoSuchProviderException, NoSuchAlgorithmException, KeyManagementException {
        if( sslContext == null ) {
            if( provider == null ) {
                sslContext = SSLContext.getInstance(protocol);
            } else {
                sslContext = SSLContext.getInstance(protocol, provider);
            }
            sslContext.init(getKeyManagersAsArray(), getTrustManagersAsArray(), getSecureRandom());
        }
        return sslContext;
    }
    public void setSSLContext(SSLContext sslContext) {
        this.sslContext = sslContext;
    }
    
    
}
