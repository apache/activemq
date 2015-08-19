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
package org.apache.activemq.transport;

import javax.net.ssl.SSLContext;

import org.apache.activemq.broker.SslContext;
import org.apache.activemq.util.IntrospectionSupport;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.util.ssl.SslContextFactory;

public class SecureSocketConnectorFactory extends SocketConnectorFactory {

    private String keyPassword = System.getProperty("javax.net.ssl.keyPassword");
    private String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
    private String keyStore = System.getProperty("javax.net.ssl.keyStore");
    private String trustStorePassword = System.getProperty("javax.net.ssl.trustStorePassword");
    private String trustStore = System.getProperty("javax.net.ssl.trustStore");
    private boolean needClientAuth;
    private boolean wantClientAuth;
    private String keyStoreType;
    private String secureRandomCertficateAlgorithm;
    private String trustCertificateAlgorithm;
    private String keyCertificateAlgorithm;
    private String protocol;
    private String auth;

    private SslContext context;
    private SslContextFactory contextFactory;

    public SecureSocketConnectorFactory() {

    }
    public SecureSocketConnectorFactory(SslContext context) {
        this.context = context;
    }

    public SecureSocketConnectorFactory(SslContextFactory contextFactory) {
        this.contextFactory = contextFactory;
    }

    @Override
    public Connector createConnector(Server server) throws Exception {
        if (getTransportOptions() != null) {
            IntrospectionSupport.setProperties(this, getTransportOptions());
        }

        SSLContext sslContext = context == null ? null : context.getSSLContext();

        // Get a reference to the current ssl context factory...

        SslContextFactory factory;
        if (contextFactory == null) {
            factory = new SslContextFactory();
            if (context != null) {
                // Should not be using this method since it does not use all of the values
                // from the passed SslContext instance.....
                factory.setSslContext(sslContext);

            } else {
                if (keyStore != null) {
                    factory.setKeyStorePath(keyStore);
                }
                if (keyStorePassword != null) {
                    factory.setKeyStorePassword(keyStorePassword);
                }
                // if the keyPassword hasn't been set, default it to the
                // key store password
                if (keyPassword == null && keyStorePassword != null) {
                    factory.setKeyStorePassword(keyStorePassword);
                }
                if (keyStoreType != null) {
                    factory.setKeyStoreType(keyStoreType);
                }
                if (secureRandomCertficateAlgorithm != null) {
                    factory.setSecureRandomAlgorithm(secureRandomCertficateAlgorithm);
                }
                if (keyCertificateAlgorithm != null) {
                    factory.setSslKeyManagerFactoryAlgorithm(keyCertificateAlgorithm);
                }
                if (trustCertificateAlgorithm != null) {
                    factory.setTrustManagerFactoryAlgorithm(trustCertificateAlgorithm);
                }
                if (protocol != null) {
                    factory.setProtocol(protocol);
                }
                if (trustStore != null) {
                    setTrustStore(factory, trustStore);
                }
                if (trustStorePassword != null) {
                    factory.setTrustStorePassword(trustStorePassword);
                }
            }
            factory.setNeedClientAuth(needClientAuth);
            factory.setWantClientAuth(wantClientAuth);
        } else {
            factory = contextFactory;
        }


        if ("KRB".equals(auth) || "BOTH".equals(auth)
            && Server.getVersion().startsWith("8")) {
            //return new Krb5AndCertsSslSocketConnector(factory, auth);
            return null;
        } else {
            ServerConnector connector = new ServerConnector(server, factory);
            server.setStopTimeout(500);
            connector.setStopTimeout(500);
            return connector;
        }
    }
    private void setTrustStore(SslContextFactory factory, String trustStore2) throws Exception {
        String mname = Server.getVersion().startsWith("8") ? "setTrustStore" : "setTrustStorePath";
        factory.getClass().getMethod(mname, String.class).invoke(factory, trustStore2);
    }



    // Properties
    // --------------------------------------------------------------------------------

    public String getKeyStore() {
        return keyStore;
    }

    public void setKeyStore(String keyStore) {
        this.keyStore = keyStore;
    }

    public String getKeyPassword() {
        return keyPassword;
    }

    public void setKeyPassword(String keyPassword) {
        this.keyPassword = keyPassword;
    }

    public String getKeyStoreType() {
        return keyStoreType;
    }

    public void setKeyStoreType(String keyStoreType) {
        this.keyStoreType = keyStoreType;
    }

    public String getKeyStorePassword() {
        return keyStorePassword;
    }

    public void setKeyStorePassword(String keyStorePassword) {
        this.keyStorePassword = keyStorePassword;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public String getSecureRandomCertficateAlgorithm() {
        return secureRandomCertficateAlgorithm;
    }

    public void setSecureRandomCertficateAlgorithm(String secureRandomCertficateAlgorithm) {
        this.secureRandomCertficateAlgorithm = secureRandomCertficateAlgorithm;
    }

    public String getKeyCertificateAlgorithm() {
        return keyCertificateAlgorithm;
    }

    public void setKeyCertificateAlgorithm(String keyCertificateAlgorithm) {
        this.keyCertificateAlgorithm = keyCertificateAlgorithm;
    }

    public String getTrustCertificateAlgorithm() {
        return trustCertificateAlgorithm;
    }

    public void setTrustCertificateAlgorithm(String trustCertificateAlgorithm) {
        this.trustCertificateAlgorithm = trustCertificateAlgorithm;
    }

    /**
     * @return the auth
     */
    public String getAuth() {
        return auth;
    }

    /**
     * @param auth the auth to set
     */
    public void setAuth(String auth) {
        this.auth = auth;
    }

    public boolean isWantClientAuth() {
        return wantClientAuth;
    }

    public void setWantClientAuth(boolean wantClientAuth) {
        this.wantClientAuth = wantClientAuth;
    }

    public boolean isNeedClientAuth() {
        return needClientAuth;
    }

    public void setNeedClientAuth(boolean needClientAuth) {
        this.needClientAuth = needClientAuth;
    }

    public String getTrustStore() {
        return trustStore;
    }

    public void setTrustStore(String trustStore) {
        this.trustStore = trustStore;
    }

    public String getTrustStorePassword() {
        return trustStorePassword;
    }

    public void setTrustStorePassword(String trustStorePassword) {
        this.trustStorePassword = trustStorePassword;
    }
}
