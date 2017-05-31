/*
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
package org.apache.activemq.transport.amqp.client.transport;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509ExtendedKeyManager;
import javax.net.ssl.X509TrustManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.handler.ssl.SslHandler;

/**
 * Static class that provides various utility methods used by Transport implementations.
 */
public class NettyTransportSupport {

    private static final Logger LOG = LoggerFactory.getLogger(NettyTransportSupport.class);

    /**
     * Creates a Netty SslHandler instance for use in Transports that require
     * an SSL encoder / decoder.
     *
     * @param remote
     *        The URI of the remote peer that the SslHandler will be used against.
     * @param options
     *        The SSL options object to build the SslHandler instance from.
     *
     * @return a new SslHandler that is configured from the given options.
     *
     * @throws Exception if an error occurs while creating the SslHandler instance.
     */
    public static SslHandler createSslHandler(URI remote, NettyTransportSslOptions options) throws Exception {
        return new SslHandler(createSslEngine(remote, createSslContext(options), options));
    }

    /**
     * Create a new SSLContext using the options specific in the given TransportSslOptions
     * instance.
     *
     * @param options
     *        the configured options used to create the SSLContext.
     *
     * @return a new SSLContext instance.
     *
     * @throws Exception if an error occurs while creating the context.
     */
    public static SSLContext createSslContext(NettyTransportSslOptions options) throws Exception {
        try {
            String contextProtocol = options.getContextProtocol();
            LOG.trace("Getting SSLContext instance using protocol: {}", contextProtocol);

            SSLContext context = SSLContext.getInstance(contextProtocol);
            KeyManager[] keyMgrs = loadKeyManagers(options);
            TrustManager[] trustManagers = loadTrustManagers(options);

            context.init(keyMgrs, trustManagers, new SecureRandom());
            return context;
        } catch (Exception e) {
            LOG.error("Failed to create SSLContext: {}", e, e);
            throw e;
        }
    }

    /**
     * Create a new SSLEngine instance in client mode from the given SSLContext and
     * TransportSslOptions instances.
     *
     * @param context
     *        the SSLContext to use when creating the engine.
     * @param options
     *        the TransportSslOptions to use to configure the new SSLEngine.
     *
     * @return a new SSLEngine instance in client mode.
     *
     * @throws Exception if an error occurs while creating the new SSLEngine.
     */
    public static SSLEngine createSslEngine(SSLContext context, NettyTransportSslOptions options) throws Exception {
        return createSslEngine(null, context, options);
    }

    /**
     * Create a new SSLEngine instance in client mode from the given SSLContext and
     * TransportSslOptions instances.
     *
     * @param remote
     *        the URI of the remote peer that will be used to initialize the engine, may be null if none should.
     * @param context
     *        the SSLContext to use when creating the engine.
     * @param options
     *        the TransportSslOptions to use to configure the new SSLEngine.
     *
     * @return a new SSLEngine instance in client mode.
     *
     * @throws Exception if an error occurs while creating the new SSLEngine.
     */
    public static SSLEngine createSslEngine(URI remote, SSLContext context, NettyTransportSslOptions options) throws Exception {
        SSLEngine engine = null;
        if(remote == null) {
            engine = context.createSSLEngine();
        } else {
            engine = context.createSSLEngine(remote.getHost(), remote.getPort());
        }

        engine.setEnabledProtocols(buildEnabledProtocols(engine, options));
        engine.setEnabledCipherSuites(buildEnabledCipherSuites(engine, options));
        engine.setUseClientMode(true);

        if (options.isVerifyHost()) {
            SSLParameters sslParameters = engine.getSSLParameters();
            sslParameters.setEndpointIdentificationAlgorithm("HTTPS");
            engine.setSSLParameters(sslParameters);
        }

        return engine;
    }

    private static String[] buildEnabledProtocols(SSLEngine engine, NettyTransportSslOptions options) {
        List<String> enabledProtocols = new ArrayList<String>();

        if (options.getEnabledProtocols() != null) {
            List<String> configuredProtocols = Arrays.asList(options.getEnabledProtocols());
            LOG.trace("Configured protocols from transport options: {}", configuredProtocols);
            enabledProtocols.addAll(configuredProtocols);
        } else {
            List<String> engineProtocols = Arrays.asList(engine.getEnabledProtocols());
            LOG.trace("Default protocols from the SSLEngine: {}", engineProtocols);
            enabledProtocols.addAll(engineProtocols);
        }

        String[] disabledProtocols = options.getDisabledProtocols();
        if (disabledProtocols != null) {
            List<String> disabled = Arrays.asList(disabledProtocols);
            LOG.trace("Disabled protocols: {}", disabled);
            enabledProtocols.removeAll(disabled);
        }

        LOG.trace("Enabled protocols: {}", enabledProtocols);

        return enabledProtocols.toArray(new String[0]);
    }

    private static String[] buildEnabledCipherSuites(SSLEngine engine, NettyTransportSslOptions options) {
        List<String> enabledCipherSuites = new ArrayList<String>();

        if (options.getEnabledCipherSuites() != null) {
            List<String> configuredCipherSuites = Arrays.asList(options.getEnabledCipherSuites());
            LOG.trace("Configured cipher suites from transport options: {}", configuredCipherSuites);
            enabledCipherSuites.addAll(configuredCipherSuites);
        } else {
            List<String> engineCipherSuites = Arrays.asList(engine.getEnabledCipherSuites());
            LOG.trace("Default cipher suites from the SSLEngine: {}", engineCipherSuites);
            enabledCipherSuites.addAll(engineCipherSuites);
        }

        String[] disabledCipherSuites = options.getDisabledCipherSuites();
        if (disabledCipherSuites != null) {
            List<String> disabled = Arrays.asList(disabledCipherSuites);
            LOG.trace("Disabled cipher suites: {}", disabled);
            enabledCipherSuites.removeAll(disabled);
        }

        LOG.trace("Enabled cipher suites: {}", enabledCipherSuites);

        return enabledCipherSuites.toArray(new String[0]);
    }

    private static TrustManager[] loadTrustManagers(NettyTransportSslOptions options) throws Exception {
        if (options.isTrustAll()) {
            return new TrustManager[] { createTrustAllTrustManager() };
        }

        if (options.getTrustStoreLocation() == null) {
            return null;
        }

        TrustManagerFactory fact = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());

        String storeLocation = options.getTrustStoreLocation();
        String storePassword = options.getTrustStorePassword();
        String storeType = options.getStoreType();

        LOG.trace("Attempt to load TrustStore from location {} of type {}", storeLocation, storeType);

        KeyStore trustStore = loadStore(storeLocation, storePassword, storeType);
        fact.init(trustStore);

        return fact.getTrustManagers();
    }

    private static KeyManager[] loadKeyManagers(NettyTransportSslOptions options) throws Exception {
        if (options.getKeyStoreLocation() == null) {
            return null;
        }

        KeyManagerFactory fact = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

        String storeLocation = options.getKeyStoreLocation();
        String storePassword = options.getKeyStorePassword();
        String storeType = options.getStoreType();
        String alias = options.getKeyAlias();

        LOG.trace("Attempt to load KeyStore from location {} of type {}", storeLocation, storeType);

        KeyStore keyStore = loadStore(storeLocation, storePassword, storeType);
        fact.init(keyStore, storePassword != null ? storePassword.toCharArray() : null);

        if (alias == null) {
            return fact.getKeyManagers();
        } else {
            validateAlias(keyStore, alias);
            return wrapKeyManagers(alias, fact.getKeyManagers());
        }
    }

    private static KeyManager[] wrapKeyManagers(String alias, KeyManager[] origKeyManagers) {
        KeyManager[] keyManagers = new KeyManager[origKeyManagers.length];
        for (int i = 0; i < origKeyManagers.length; i++) {
            KeyManager km = origKeyManagers[i];
            if (km instanceof X509ExtendedKeyManager) {
                km = new X509AliasKeyManager(alias, (X509ExtendedKeyManager) km);
            }

            keyManagers[i] = km;
        }

        return keyManagers;
    }

    private static void validateAlias(KeyStore store, String alias) throws IllegalArgumentException, KeyStoreException {
        if (!store.containsAlias(alias)) {
            throw new IllegalArgumentException("The alias '" + alias + "' doesn't exist in the key store");
        }

        if (!store.isKeyEntry(alias)) {
            throw new IllegalArgumentException("The alias '" + alias + "' in the keystore doesn't represent a key entry");
        }
    }

    private static KeyStore loadStore(String storePath, final String password, String storeType) throws Exception {
        KeyStore store = KeyStore.getInstance(storeType);
        try (InputStream in = new FileInputStream(new File(storePath));) {
            store.load(in, password != null ? password.toCharArray() : null);
        }

        return store;
    }

    private static TrustManager createTrustAllTrustManager() {
        return new X509TrustManager() {
            @Override
            public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            }

            @Override
            public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
            }

            @Override
            public X509Certificate[] getAcceptedIssuers() {
                return new X509Certificate[0];
            }
        };
    }
}
