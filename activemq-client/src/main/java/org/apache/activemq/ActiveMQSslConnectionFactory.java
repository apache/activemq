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

package org.apache.activemq;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.KeyStore;
import java.security.SecureRandom;

import javax.jms.JMSException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.util.JMSExceptionSupport;

/**
 * An ActiveMQConnectionFactory that allows access to the key and trust managers
 * used for SslConnections. There is no reason to use this class unless SSL is
 * being used AND the key and trust managers need to be specified from within
 * code. In fact, if the URI passed to this class does not have an "ssl" scheme,
 * this class will pass all work on to its superclass.
 *
 * There are two alternative approaches you can use to provide X.509
 * certificates for the SSL connections:
 *
 * Call <code>setTrustStore</code>, <code>setTrustStorePassword</code>,
 * <code>setKeyStore</code>, and <code>setKeyStorePassword</code>.
 *
 * Call <code>setKeyAndTrustManagers</code>.
 *
 * @author sepandm@gmail.com
 */
public class ActiveMQSslConnectionFactory extends ActiveMQConnectionFactory {

    // The key and trust managers used to initialize the used SSLContext.
    protected KeyManager[] keyManager;
    protected TrustManager[] trustManager;
    protected SecureRandom secureRandom;
    protected String trustStoreType = KeyStore.getDefaultType();
    protected String trustStore;
    protected String trustStorePassword;
    protected String keyStoreType = KeyStore.getDefaultType();
    protected String keyStore;
    protected String keyStorePassword;
    protected String keyStoreKeyPassword;

    public ActiveMQSslConnectionFactory() {
        super();
    }

    public ActiveMQSslConnectionFactory(String brokerURL) {
        super(brokerURL);
    }

    public ActiveMQSslConnectionFactory(URI brokerURL) {
        super(brokerURL);
    }

    /**
     * Sets the key and trust managers used when creating SSL connections.
     *
     * @param km
     *            The KeyManagers used.
     * @param tm
     *            The TrustManagers used.
     * @param random
     *            The SecureRandom number used.
     */
    public void setKeyAndTrustManagers(final KeyManager[] km, final TrustManager[] tm, final SecureRandom random) {
        keyManager = km;
        trustManager = tm;
        secureRandom = random;
    }

    /**
     * Overriding to make special considerations for SSL connections. If we are
     * not using SSL, the superclass's method is called. If we are using SSL, an
     * SslConnectionFactory is used and it is given the needed key and trust
     * managers.
     *
     * @author sepandm@gmail.com
     */
    @Override
    protected Transport createTransport() throws JMSException {
        SslContext existing = SslContext.getCurrentSslContext();
        try {
            if (keyStore != null || trustStore != null) {
                keyManager = createKeyManager();
                trustManager = createTrustManager();
            }
            if (keyManager != null || trustManager != null) {
                SslContext.setCurrentSslContext(new SslContext(keyManager, trustManager, secureRandom));
            }
            return super.createTransport();
        } catch (Exception e) {
            throw JMSExceptionSupport.create("Could not create Transport. Reason: " + e, e);
        } finally {
            SslContext.setCurrentSslContext(existing);
        }
    }

    protected TrustManager[] createTrustManager() throws Exception {
        TrustManager[] trustStoreManagers = null;
        KeyStore trustedCertStore = KeyStore.getInstance(getTrustStoreType());

        if (trustStore != null) {
            try(InputStream tsStream = getInputStream(trustStore)) {

                trustedCertStore.load(tsStream, trustStorePassword.toCharArray());
                TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

                tmf.init(trustedCertStore);
                trustStoreManagers = tmf.getTrustManagers();
            }
        }
        return trustStoreManagers;
    }

    protected KeyManager[] createKeyManager() throws Exception {
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        KeyStore ks = KeyStore.getInstance(getKeyStoreType());
        KeyManager[] keystoreManagers = null;
        if (keyStore != null) {
            byte[] sslCert = loadClientCredential(keyStore);

            if (sslCert != null && sslCert.length > 0) {
                try(ByteArrayInputStream bin = new ByteArrayInputStream(sslCert)) {
                    ks.load(bin, keyStorePassword.toCharArray());
                    kmf.init(ks, keyStoreKeyPassword !=null ? keyStoreKeyPassword.toCharArray() : keyStorePassword.toCharArray());
                    keystoreManagers = kmf.getKeyManagers();
                }
            }
        }
        return keystoreManagers;
    }

    protected byte[] loadClientCredential(String fileName) throws IOException {
        if (fileName == null) {
            return null;
        }
        try(InputStream in = getInputStream(fileName);
            ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            byte[] buf = new byte[512];
            int i = in.read(buf);
            while (i > 0) {
                out.write(buf, 0, i);
                i = in.read(buf);
            }
            return out.toByteArray();
        }
    }

    protected InputStream getInputStream(String urlOrResource) throws IOException {
        try {
            File ifile = new File(urlOrResource);
            // only open the file if and only if it exists
            if (ifile.exists()) {
                return new FileInputStream(ifile);
            }
        } catch (Exception e) {
        }

        InputStream ins = null;

        try {
            URL url = new URL(urlOrResource);
            ins = url.openStream();
            if (ins != null) {
                return ins;
            }
        } catch (MalformedURLException ignore) {
        }

        // Alternatively, treat as classpath resource
        if (ins == null) {
            ins = Thread.currentThread().getContextClassLoader().getResourceAsStream(urlOrResource);
        }

        if (ins == null) {
            throw new IOException("Could not load resource: " + urlOrResource);
        }

        return ins;
    }

    public String getTrustStoreType() {
        return trustStoreType;
    }

    public void setTrustStoreType(String type) {
        trustStoreType = type;
    }

    public String getTrustStore() {
        return trustStore;
    }

    /**
     * The location of a keystore file (in <code>jks</code> format) containing
     * one or more trusted certificates.
     *
     * @param trustStore
     *            If specified with a scheme, treat as a URL, otherwise treat as
     *            a classpath resource.
     */
    public void setTrustStore(String trustStore) throws Exception {
        this.trustStore = trustStore;
        trustManager = null;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    /**
     * The password to match the trust store specified by {@link setTrustStore}.
     *
     * @param trustStorePassword
     *            The password used to unlock the keystore file.
     */
    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public void setKeyStoreType(String type) {
        keyStoreType = type;
    }


    public String getKeyStore() {
        return keyStore;
    }

    /**
     * The location of a keystore file (in <code>jks</code> format) containing a
     * certificate and its private key.
     *
     * @param keyStore
     *            If specified with a scheme, treat as a URL, otherwise treat as
     *            a classpath resource.
     */
    public void setKeyStore(String keyStore) throws Exception {
        this.keyStore = keyStore;
        keyManager = null;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    /**
     * The password to match the key store specified by {@link setKeyStore}.
     *
     * @param keyStorePassword
     *            The password, which is used both to unlock the keystore file
     *            and as the pass phrase for the private key stored in the
     *            keystore.
     */
    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }


    public String getKeyStoreKeyPassword() {
        return keyStoreKeyPassword;
    }

    /**
     * The password to match the key from the keyStore.
     *
     * @param keyStoreKeyPassword
     *            The password for the private key stored in the
     *            keyStore if different from keyStorePassword.
     */
    public void setKeyStoreKeyPassword(String keyStoreKeyPassword) {
        this.keyStoreKeyPassword = keyStoreKeyPassword;
    }

}
