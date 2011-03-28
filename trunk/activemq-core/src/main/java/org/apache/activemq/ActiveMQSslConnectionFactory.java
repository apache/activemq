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
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.KeyStore;

import java.net.MalformedURLException;
import java.net.URI;
import java.net.URL;
import java.security.SecureRandom;
import javax.jms.JMSException;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.transport.Transport;
import org.apache.activemq.transport.tcp.SslTransportFactory;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * An ActiveMQConnectionFactory that allows access to the key and trust managers
 * used for SslConnections. There is no reason to use this class unless SSL is
 * being used AND the key and trust managers need to be specified from within
 * code. In fact, if the URI passed to this class does not have an "ssl" scheme,
 * this class will pass all work on to its superclass.
 * 
 * There are two alternative approaches you can use to provide X.509 certificates
 * for the SSL connections:
 * 
 * Call <code>setTrustStore</code>, <code>setTrustStorePassword</code>, <code>setKeyStore</code>,
 * and <code>setKeyStorePassword</code>.
 * 
 * Call <code>setKeyAndTrustManagers</code>.
 * 
 * @author sepandm@gmail.com
 */
public class ActiveMQSslConnectionFactory extends ActiveMQConnectionFactory {
    private static final Log LOG = LogFactory.getLog(ActiveMQSslConnectionFactory.class);
    // The key and trust managers used to initialize the used SSLContext.
    protected KeyManager[] keyManager;
    protected TrustManager[] trustManager;
    protected SecureRandom secureRandom;
    protected String trustStore;
    protected String trustStorePassword;
    protected String keyStore;
    protected String keyStorePassword;

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
     * @param km The KeyManagers used.
     * @param tm The TrustManagers used.
     * @param random The SecureRandom number used.
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
    protected Transport createTransport() throws JMSException {
        // If the given URI is non-ssl, let superclass handle it.
        if (!brokerURL.getScheme().equals("ssl")) {
            return super.createTransport();
        }

        try {
            if (keyManager == null || trustManager == null) {
                trustManager = createTrustManager();
                keyManager = createKeyManager();
                // secureRandom can be left as null
            }
            SslTransportFactory sslFactory = new SslTransportFactory();
            SslContext ctx = new SslContext(keyManager, trustManager, secureRandom);
            SslContext.setCurrentSslContext(ctx);
            return sslFactory.doConnect(brokerURL);
        } catch (Exception e) {
            throw JMSExceptionSupport.create("Could not create Transport. Reason: " + e, e);
        }
    }

    protected TrustManager[] createTrustManager() throws Exception {
        TrustManager[] trustStoreManagers = null;
        KeyStore trustedCertStore = KeyStore.getInstance("jks");
        
        InputStream tsStream = getUrlOrResourceAsStream(trustStore);
        
        trustedCertStore.load(tsStream, trustStorePassword.toCharArray());
        TrustManagerFactory tmf  = 
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
  
        tmf.init(trustedCertStore);
        trustStoreManagers = tmf.getTrustManagers();
        return trustStoreManagers; 
    }

    protected KeyManager[] createKeyManager() throws Exception {
        KeyManagerFactory kmf = 
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());  
        KeyStore ks = KeyStore.getInstance("jks");
        KeyManager[] keystoreManagers = null;
        
        byte[] sslCert = loadClientCredential(keyStore);
        
       
        if (sslCert != null && sslCert.length > 0) {
            ByteArrayInputStream bin = new ByteArrayInputStream(sslCert);
            ks.load(bin, keyStorePassword.toCharArray());
            kmf.init(ks, keyStorePassword.toCharArray());
            keystoreManagers = kmf.getKeyManagers();
        }
        return keystoreManagers;          
    }

    protected byte[] loadClientCredential(String fileName) throws IOException {
        if (fileName == null) {
            return null;
        }
        InputStream in = getUrlOrResourceAsStream(fileName);
        //FileInputStream in = new FileInputStream(fileName);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buf = new byte[512];
        int i = in.read(buf);
        while (i  > 0) {
            out.write(buf, 0, i);
            i = in.read(buf);
        }
        in.close();
        return out.toByteArray();
    }
    
    protected InputStream getUrlOrResourceAsStream(String urlOrResource) throws IOException {
    	InputStream ins = null;
    	try {
    		URL url = new URL(urlOrResource);
    		ins = url.openStream();
    	}
    	catch (MalformedURLException ignore) {
    		ins = null;
    	}
    	
    	// Alternatively, treat as classpath resource
    	if (ins == null) {
        	ins = getClass().getClassLoader().getResourceAsStream(urlOrResource);
    	}
    	
    	if (ins == null) {
            throw new java.io.IOException("Could not load resource: " + urlOrResource);
    	}
    	
    	return ins;
    }
    
    public String getTrustStore() {
        return trustStore;
    }
    
    /**
     * The location of a keystore file (in <code>jks</code> format) containing one or more
     * trusted certificates.
     * 
     * @param trustStore If specified with a scheme, treat as a URL, otherwise treat as a classpath resource.
     */
    public void setTrustStore(String trustStore) {
        this.trustStore = trustStore;
        trustManager = null;
    }
    
    public String getTrustStorePassword() {
        return trustStorePassword;
    }
    
    /**
     * The password to match the trust store specified by {@link setTrustStore}.
     * 
     * @param trustStorePassword The password used to unlock the keystore file.
     */
    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }
    
    public String getKeyStore() {
        return keyStore;
    }
    
    /**
     * The location of a keystore file (in <code>jks</code> format) containing a certificate
     * and its private key.
     * 
     * @param keyStore If specified with a scheme, treat as a URL, otherwise treat as a classpath resource.
     */
    public void setKeyStore(String keyStore) {
        this.keyStore = keyStore;
        keyManager = null;
    }
    
    public String getKeyStorePassword() {
        return keyStorePassword;
    }
    
    /**
     * The password to match the key store specified by {@link setKeyStore}.
     * 
     * @param keyStorePassword The password, which is used both to unlock the keystore file
     * and as the pass phrase for the private key stored in the keystore.
     */
    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

}
