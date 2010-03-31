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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.security.KeyStore;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSession;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;

import junit.framework.Test;
import junit.textui.TestRunner;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.SslBrokerService;
import org.apache.activemq.broker.SslContext;
import org.apache.activemq.broker.TransportConnector;
import org.apache.activemq.transport.TransportBrokerTestSupport;
import org.apache.activemq.transport.TransportFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class SslBrokerServiceTest extends TransportBrokerTestSupport {
    private static final Log LOG = LogFactory.getLog(SslBrokerServiceTest.class);

    TransportConnector needClientAuthConnector;
    TransportConnector limitedCipherSuites;
    
    protected String getBindLocation() {
        return "ssl://localhost:0";
    }
    
    @Override
    protected BrokerService createBroker() throws Exception {
        SslBrokerService service = new SslBrokerService();
        service.setPersistent(false);
        
        KeyManager[] km = getKeyManager();
        TrustManager[] tm = getTrustManager();
        connector = service.addSslConnector(getBindLocation(), km, tm, null);
        limitedCipherSuites = service.addSslConnector("ssl://localhost:0?transport.enabledCipherSuites=SSL_RSA_WITH_RC4_128_SHA,SSL_DH_anon_WITH_3DES_EDE_CBC_SHA", km, tm, null);
        needClientAuthConnector = service.addSslConnector("ssl://localhost:0?transport.needClientAuth=true", km, tm, null);
        
        // for client side
        SslTransportFactory sslFactory = new SslTransportFactory();
        SslContext ctx = new SslContext(km, tm, null);
        SslContext.setCurrentSslContext(ctx);
        TransportFactory.registerTransportFactory("ssl", sslFactory);
        
        return service;
    }

    public void testNeedClientAuth() throws Exception {   
        SSLContext context = SSLContext.getInstance("TLS");    
        // no client cert
        context.init(null, getTrustManager(), null);
        
        try {
            makeSSLConnection(context, null, needClientAuthConnector);
            fail("expected failure on no client cert");
        } catch (SSLException expected) {
            expected.printStackTrace();
        }
        // should work with regular connector
        makeSSLConnection(context, null, connector);
    }
    
    public void testCipherSuitesDisabled() throws Exception {
        SSLContext context = SSLContext.getInstance("TLS");
        context.init(getKeyManager(), getTrustManager(), null);
        
        // Enable only one cipher suite which is not enabled on the server
        try {
            makeSSLConnection(context, new String[]{ "SSL_RSA_WITH_RC4_128_MD5" }, limitedCipherSuites);
            fail("expected failure on non allowed cipher suite");
        } catch (SSLException expectedOnNotAnAvailableSuite) {
        }

        // ok with the enabled one
        makeSSLConnection(context, new String[]{ "SSL_RSA_WITH_RC4_128_SHA" }, limitedCipherSuites);
    }

    private void makeSSLConnection(SSLContext context, String enabledSuites[], TransportConnector connector) throws Exception,
            UnknownHostException, SocketException {
        SSLSocket sslSocket = (SSLSocket) context.getSocketFactory().createSocket("localhost", connector.getUri().getPort());
        
        if (enabledSuites != null) {
            sslSocket.setEnabledCipherSuites(enabledSuites);
        }
        sslSocket.setSoTimeout(5000);
        
        SSLSession session = sslSocket.getSession();
        sslSocket.startHandshake();
        LOG.info("cyphersuite: " + session.getCipherSuite());
        LOG.info("peer port: " + session.getPeerPort());
        LOG.info("peer cert: " + session.getPeerCertificateChain()[0].toString());    
    }
    
    private TrustManager[] getTrustManager() throws Exception {
        TrustManager[] trustStoreManagers = null;
        KeyStore trustedCertStore = KeyStore.getInstance(SslTransportBrokerTest.KEYSTORE_TYPE);
        
        trustedCertStore.load(new FileInputStream(SslTransportBrokerTest.TRUST_KEYSTORE), null);
        TrustManagerFactory tmf  = 
            TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
  
        tmf.init(trustedCertStore);
        trustStoreManagers = tmf.getTrustManagers();
        return trustStoreManagers; 
    }

    private KeyManager[] getKeyManager() throws Exception {
        KeyManagerFactory kmf = 
            KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());  
        KeyStore ks = KeyStore.getInstance(SslTransportBrokerTest.KEYSTORE_TYPE);
        KeyManager[] keystoreManagers = null;
        
        byte[] sslCert = loadClientCredential(SslTransportBrokerTest.SERVER_KEYSTORE);
        
       
        if (sslCert != null && sslCert.length > 0) {
            ByteArrayInputStream bin = new ByteArrayInputStream(sslCert);
            ks.load(bin, SslTransportBrokerTest.PASSWORD.toCharArray());
            kmf.init(ks, SslTransportBrokerTest.PASSWORD.toCharArray());
            keystoreManagers = kmf.getKeyManagers();
        }
        return keystoreManagers;          
    }

    private static byte[] loadClientCredential(String fileName) throws IOException {
        if (fileName == null) {
            return null;
        }
        FileInputStream in = new FileInputStream(fileName);
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

    protected void setUp() throws Exception {
        maxWait = 10000;
        super.setUp();
    }

    public static Test suite() {
        return suite(SslBrokerServiceTest.class);
    }

    public static void main(String[] args) {
        TestRunner.run(suite());
    }
}
