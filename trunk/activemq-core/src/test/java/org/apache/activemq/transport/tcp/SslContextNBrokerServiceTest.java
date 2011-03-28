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

import java.net.URI;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Iterator;
import java.util.Map;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

import junit.framework.TestCase;

import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.TransportConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;


public class SslContextNBrokerServiceTest extends TestCase {
    private static final transient Logger LOG = LoggerFactory.getLogger(SslContextNBrokerServiceTest.class);
    
    private ClassPathXmlApplicationContext context;
    Map beansOfType;
    
    public void testConfigurationIsolation() throws Exception {
        
        assertTrue("dummy bean has dummy cert", verifyCredentials("dummy"));
        assertTrue("good bean has amq cert", verifyCredentials("activemq.org"));
    }
    
    private boolean verifyCredentials(String name) throws Exception {
        boolean result = false;
        BrokerService broker = getBroker(name);
        assertNotNull(name, broker);
        broker.start();
        try {
            result = verifySslCredentials(broker);
        } finally {
            broker.stop();
        }
        return result;
    }

    private boolean verifySslCredentials(BrokerService broker) throws Exception {
        TransportConnector connector = broker.getTransportConnectors().get(0);
        URI brokerUri = connector.getConnectUri();

        SSLContext context = SSLContext.getInstance("TLS");        
        CertChainCatcher catcher = new CertChainCatcher(); 
        context.init(null, new TrustManager[] {catcher}, null);
        
        SSLSocketFactory factory = context.getSocketFactory();
        LOG.info("Connecting to broker: " + broker.getBrokerName()
                + " on: " + brokerUri.getHost() + ":" + brokerUri.getPort());
        SSLSocket socket = (SSLSocket)factory.createSocket(brokerUri.getHost(), brokerUri.getPort());
        socket.setSoTimeout(5000);
        socket.startHandshake();
        socket.close();
        
        boolean matches = false;
        if (catcher.serverCerts != null) {
            for (int i = 0; i < catcher.serverCerts.length; i++) {
                X509Certificate cert = catcher.serverCerts[i];
                LOG.info(" " + (i + 1) + " Issuer " + cert.getIssuerDN());
            }
            if (catcher.serverCerts.length > 0) {
                String issuer = catcher.serverCerts[0].getIssuerDN().toString();
                if (issuer.indexOf(broker.getBrokerName()) != -1) {
                    matches = true;
                }
            }
        }
        return matches; 
    }


    private BrokerService getBroker(String name) {
        BrokerService result = null;
        Iterator iterator = beansOfType.values().iterator();
        while(iterator.hasNext()) {
            BrokerService candidate = (BrokerService)iterator.next();
            if (candidate.getBrokerName().equals(name)) {
                result = candidate;
                break;
            }
        }
        return result;
    }


    protected void setUp() throws Exception {     
        //System.setProperty("javax.net.debug", "ssl");
        Thread.currentThread().setContextClassLoader(SslContextNBrokerServiceTest.class.getClassLoader());
        context = new ClassPathXmlApplicationContext("org/apache/activemq/transport/tcp/n-brokers-ssl.xml");
        beansOfType = context.getBeansOfType(BrokerService.class);
        
    }
    
    @Override
    protected void tearDown() throws Exception {
        context.destroy();
    }


    class CertChainCatcher implements  X509TrustManager {  
        X509Certificate[] serverCerts;
        
        public void checkClientTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
        }
        public void checkServerTrusted(X509Certificate[] arg0, String arg1) throws CertificateException {
            serverCerts = arg0;
        }
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }
    }
}
