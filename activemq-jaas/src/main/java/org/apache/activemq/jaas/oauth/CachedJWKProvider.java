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
package org.apache.activemq.jaas.oauth;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.jwk.JWK;
import com.nimbusds.jose.jwk.JWKSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.security.PublicKey;
import java.text.ParseException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class CachedJWKProvider implements JWKProvider {

    private static final Logger LOG = LoggerFactory.getLogger(CachedJWKProvider.class);

    private final URI jwksUri;
    private final Map<String, PublicKey> knownKeys = new HashMap<>();
    private final ReadWriteLock knownKeysLock = new ReentrantReadWriteLock();

    public CachedJWKProvider(final URI jwksUri) {
        this.jwksUri = jwksUri;
    }

    @Override
    public Optional<PublicKey> getKey(String kid) {
        knownKeysLock.readLock().lock();
        PublicKey publicKey = knownKeys.get(kid);
        knownKeysLock.readLock().unlock();

        if (publicKey != null) {
            // Key is present in the cache
            LOG.info("Key ID {} found in the local cache", kid);
            return Optional.of(publicKey);
        }

        LOG.info("Key ID {} not found in the local cache, will download the JWKS document from {}", kid, jwksUri);

        // TODO: It would be better to avoid holding the lock while downloading the document.
        knownKeysLock.writeLock().lock();

        // Optimistically check again in case the key was downloaded by a different thread by a previously in-flight
        // request.
        publicKey = knownKeys.get(kid);
        if (publicKey == null) {
            // No, the key was no populated and has to be downloaded.
            loadKeys();
            publicKey = knownKeys.get(kid);
        } else {
            LOG.info("Key ID {} is now present in the local cache, will skip downloading the JWKS document again", kid);
        }

        knownKeysLock.writeLock().unlock();

        return Optional.ofNullable(publicKey);
    }

    private void loadKeys() {
        try {
            final String response = downloadJwksDocument();
            LOG.info("Got JWKS '{}'", response);

            JWKSet.parse(response)
                .getKeys()
                .forEach(this::storeKey);
        } catch (ParseException parseEx) {
            LOG.error("Failed to parse the JWKS from '{}': {}", jwksUri, parseEx.getMessage());
        } catch (Exception ex) {
            LOG.error("Failed to get the JWKS from '{}': {}", jwksUri, ex.getMessage());
        }
    }

    private String downloadJwksDocument() throws IOException, InterruptedException {
        final HttpClient httpClient = HttpClient
                .newBuilder()
                .connectTimeout(Duration.ofSeconds(10))
                .build();

        final HttpRequest request = HttpRequest.newBuilder()
                .uri(jwksUri)
                .timeout(Duration.ofSeconds(10))
                .build();

        LOG.info("Downloading JWKS from '{}'", jwksUri);

        // TODO: let timeout be configurable, implement a retry strategy

        final HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());

        if (response.statusCode() != 200) {
            throw new IOException(String.format(
                    "Failed to fetch JWKS from '%s'. Got status code: %d",
                    jwksUri,
                    response.statusCode()));
        }

        return response.body();
    }

    private void storeKey(final JWK key) {
        try {
            final String algorithm = key.getAlgorithm().getName();
            switch (algorithm) {
                // TODO: Double check the correct algorithm names
                case "RS256":
                    knownKeys.put(key.getKeyID(), key.toRSAKey().toPublicKey());
                    break;
                case "EC": // likely incorrect
                    knownKeys.put(key.getKeyID(), key.toECKey().toPublicKey());
                default:
                    LOG.warn("Unsupported key algorithm: '{}'", algorithm);
            }
        } catch (JOSEException joseEx) {
            LOG.error("Failed to parse key: {}", joseEx.getMessage());
        }
    }
}
