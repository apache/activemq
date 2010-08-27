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
package org.apache.activemq.transport.https;

import java.net.URI;

import org.apache.activemq.transport.http.HttpTransportServer;
import org.eclipse.jetty.server.ssl.SslSocketConnector;

public class HttpsTransportServer extends HttpTransportServer {

    private String keyPassword = System.getProperty("javax.net.ssl.keyPassword");
    private String keyStorePassword = System.getProperty("javax.net.ssl.keyStorePassword");
    private String keyStore = System.getProperty("javax.net.ssl.keyStore");
    private String keyStoreType;
    private String secureRandomCertficateAlgorithm;
    private String trustCertificateAlgorithm;
    private String keyCertificateAlgorithm;
    private String protocol;

    public HttpsTransportServer(URI uri, HttpsTransportFactory factory) {
        super(uri, factory);
    }

    public void doStart() throws Exception {
        SslSocketConnector sslConnector = new SslSocketConnector();
        sslConnector.setKeystore(keyStore);
        sslConnector.setPassword(keyStorePassword);
        // if the keyPassword hasn't been set, default it to the
        // key store password
        if (keyPassword == null) {
            sslConnector.setKeyPassword(keyStorePassword);
        }
        if (keyStoreType != null) {
            sslConnector.setKeystoreType(keyStoreType);
        }
        if (secureRandomCertficateAlgorithm != null) {
            sslConnector.setSecureRandomAlgorithm(secureRandomCertficateAlgorithm);
        }
        if (keyCertificateAlgorithm != null) {
            sslConnector.setSslKeyManagerFactoryAlgorithm(keyCertificateAlgorithm);
        }
        if (trustCertificateAlgorithm != null) {
            sslConnector.setSslTrustManagerFactoryAlgorithm(trustCertificateAlgorithm);
        }
        if (protocol != null) {
            sslConnector.setProtocol(protocol);
        }

        setConnector(sslConnector);

        super.doStart();
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

}
