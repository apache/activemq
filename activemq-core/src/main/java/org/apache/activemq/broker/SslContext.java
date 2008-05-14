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

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.TrustManager;

/**
 * A holder of SSL configuration.
 */
public class SslContext {
    
    protected List<KeyManager> keyManagers = new ArrayList<KeyManager>();
    protected List<TrustManager> trustManagers = new ArrayList<TrustManager>();
    protected SecureRandom secureRandom;
    
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
        
}
