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
package org.apache.activemq.spring;

import java.io.InputStream;
import java.net.MalformedURLException;
import java.security.KeyStore;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.Security;
import java.security.cert.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

import javax.annotation.PostConstruct;
import javax.net.ssl.*;

import org.apache.activemq.broker.SslContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.io.Resource;

/**
 * Extends the SslContext so that it's easier to configure from spring.
 *
 * @org.apache.xbean.XBean element="sslContext"
 *
 *
 */
public class SpringSslContext extends SslContext {

    private static final transient Logger LOG = LoggerFactory.getLogger(SpringSslContext.class);

    private String keyStoreType="jks";
    private String trustStoreType="jks";

    private String secureRandomAlgorithm="SHA1PRNG";
    private String keyStoreAlgorithm=KeyManagerFactory.getDefaultAlgorithm();
    private String trustStoreAlgorithm=TrustManagerFactory.getDefaultAlgorithm();

    private String keyStore;
    private String trustStore;

    private String keyStoreKeyPassword;
    private String keyStorePassword;
    private String trustStorePassword;

    private String crlPath;

    /**
     * JSR-250 callback wrapper; converts checked exceptions to runtime exceptions
     *
     * delegates to afterPropertiesSet, done to prevent backwards incompatible signature change.
     */
    @PostConstruct
    private void postConstruct() {
        try {
            afterPropertiesSet();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    /**
     *
     * @throws Exception
     * @org.apache.xbean.InitMethod
     */
    public void afterPropertiesSet() throws Exception {
        keyManagers.addAll(createKeyManagers());
        trustManagers.addAll(createTrustManagers());
        if( secureRandom == null ) {
            secureRandom = createSecureRandom();
        }
    }

    private SecureRandom createSecureRandom() throws NoSuchAlgorithmException {
        return SecureRandom.getInstance(secureRandomAlgorithm);
    }

    private Collection<TrustManager> createTrustManagers() throws Exception {
        boolean ocsp = Boolean.valueOf(Security.getProperty("ocsp.enable"));

        KeyStore ks = createTrustManagerKeyStore();
        if( ks ==null ) {
            return new ArrayList<TrustManager>(0);
        }
        TrustManagerFactory tmf  = TrustManagerFactory.getInstance(trustStoreAlgorithm);
        boolean initialized = false;
        if ((ocsp || crlPath != null) && trustStoreAlgorithm.equalsIgnoreCase("PKIX")) {
            PKIXBuilderParameters pkixParams = new PKIXBuilderParameters(ks, new X509CertSelector());
            if (crlPath != null) {
                pkixParams.setRevocationEnabled(true);
                Collection<? extends CRL> crlList = loadCRL();
                if (crlList != null) {
                    pkixParams.addCertStore(CertStore.getInstance("Collection", new CollectionCertStoreParameters(crlList)));
                }
            }
            tmf.init(new CertPathTrustManagerParameters(pkixParams));
            initialized = true;
        }

        if (!initialized) {
            tmf.init(ks);
        }

        return Arrays.asList(tmf.getTrustManagers());
    }

    private Collection<KeyManager> createKeyManagers() throws Exception {
        KeyStore ks = createKeyManagerKeyStore();
        if( ks ==null ) {
            return new ArrayList<KeyManager>(0);
        }

        KeyManagerFactory tmf  = KeyManagerFactory.getInstance(keyStoreAlgorithm);
        tmf.init(ks, keyStoreKeyPassword == null ? (keyStorePassword==null? null : keyStorePassword.toCharArray()) : keyStoreKeyPassword.toCharArray());
        return Arrays.asList(tmf.getKeyManagers());
    }

    private KeyStore createTrustManagerKeyStore() throws Exception {
        if( trustStore ==null ) {
            return null;
        }

        KeyStore ks = KeyStore.getInstance(trustStoreType);
        InputStream is=Utils.resourceFromString(trustStore).getInputStream();
        try {
            ks.load(is, trustStorePassword==null? null : trustStorePassword.toCharArray());
        } finally {
            is.close();
        }
        return ks;
    }

    private KeyStore createKeyManagerKeyStore() throws Exception {
        if( keyStore ==null ) {
            return null;
        }

        KeyStore ks = KeyStore.getInstance(keyStoreType);
        InputStream is=Utils.resourceFromString(keyStore).getInputStream();
        try {
            ks.load(is, keyStorePassword==null? null : keyStorePassword.toCharArray());
        } finally {
            is.close();
        }
        return ks;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public String getKeyStore() {
        return keyStore;
    }

    public void setKeyStore(String keyStore) throws MalformedURLException {
        this.keyStore = keyStore;
    }

    public String getTrustStore() {
        return trustStore;
    }

    public void setTrustStore(String trustStore) throws MalformedURLException {
        this.trustStore = trustStore;
    }

    public String getKeyStoreAlgorithm() {
        return keyStoreAlgorithm;
    }

    public void setKeyStoreAlgorithm(String keyAlgorithm) {
        this.keyStoreAlgorithm = keyAlgorithm;
    }

    public String getTrustStoreAlgorithm() {
        return trustStoreAlgorithm;
    }

    public void setTrustStoreAlgorithm(String trustAlgorithm) {
        this.trustStoreAlgorithm = trustAlgorithm;
    }

    public String getKeyStoreKeyPassword() {
        return keyStoreKeyPassword;
    }

    public void setKeyStoreKeyPassword(String keyPassword) {
        this.keyStoreKeyPassword = keyPassword;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyPassword) {
        this.keyStorePassword = keyPassword;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustPassword) {
        this.trustStorePassword = trustPassword;
    }

    public void setKeyStoreType(String keyType) {
        this.keyStoreType = keyType;
    }

    public void setTrustStoreType(String trustType) {
        this.trustStoreType = trustType;
    }

    public String getSecureRandomAlgorithm() {
        return secureRandomAlgorithm;
    }

    public void setSecureRandomAlgorithm(String secureRandomAlgorithm) {
        this.secureRandomAlgorithm = secureRandomAlgorithm;
    }

    public String getCrlPath() {
        return crlPath;
    }

    public void setCrlPath(String crlPath) {
        this.crlPath = crlPath;
    }

    private Collection<? extends CRL> loadCRL() throws Exception {
        if (crlPath == null) {
            return null;
        }
        Resource resource = Utils.resourceFromString(crlPath);
        InputStream is = resource.getInputStream();
        try {
            return CertificateFactory.getInstance("X.509").generateCRLs(is);
        } finally {
            is.close();
        }
    }

}
