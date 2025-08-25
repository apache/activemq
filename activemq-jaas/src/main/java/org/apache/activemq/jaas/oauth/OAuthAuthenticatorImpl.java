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
import com.nimbusds.jose.JWSVerifier;
import com.nimbusds.jose.crypto.ECDSAVerifier;
import com.nimbusds.jose.crypto.RSASSAVerifier;
import com.nimbusds.jwt.JWT;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.login.CredentialExpiredException;
import javax.security.auth.login.FailedLoginException;
import java.security.PublicKey;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.text.ParseException;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Optional;

public class OAuthAuthenticatorImpl implements OAuthAuthenticator {

    private static final Logger LOG = LoggerFactory.getLogger(OAuthAuthenticatorImpl.class);

    private final JWKProvider keyProvider;
    private final String issuer;
    private final String audience;

    public OAuthAuthenticatorImpl(final JWKProvider keyProvider, final String issuer, final String audience) {
        this.keyProvider = keyProvider;
        this.issuer = issuer;
        this.audience = audience;
    }

    @Override
    public AuthenticationResult authenticate(final String token) throws FailedLoginException, CredentialExpiredException {
        LOG.info("Authenticating token '{}'", token);

        final SignedJWT jwt = tryParse(token);

        verifySignature(jwt);

        LOG.info("Signature is valid");

        final JWTClaimsSet claims = tryParseClaims(jwt);

        verifyClaims(claims);

        LOG.info("Claims are valid");

        final String user = getUser(claims); // May be null
        final List<String> scopes = tryParseScopes(claims); // May be empty, but not null

        return new AuthenticationResult(user, scopes);
    }

    private SignedJWT tryParse(final String token) throws FailedLoginException {
        try {
            return SignedJWT.parse(token);
        } catch (final ParseException parseException) {
            LOG.error("Failed to parse token: {}", parseException.getMessage());
            throw new FailedLoginException();
        }
    }

    private JWTClaimsSet tryParseClaims(final JWT token) throws FailedLoginException {
        try {
            return token.getJWTClaimsSet();
        } catch (ParseException parseException) {
            LOG.error("Failed to parse token claims: {}", parseException.getMessage());
            throw new FailedLoginException();
        }
    }

    private String getUser(final JWTClaimsSet claims) {
        // TODO: the "user" identification might come from different claims
        // (see https://github.com/apache/activemq/pull/1035)
        return claims.getSubject();
    }

    private List<String> tryParseScopes(final JWTClaimsSet claims) throws FailedLoginException {
        try {
            // TODO: This code is incomplete, it must parse a list of scopes.
            final String scope = claims.getStringClaim("scope");
            if (scope == null || scope.isBlank()) {
                LOG.warn("Token does not contain any scope claim");
                return Collections.emptyList();
            }
            return List.of(scope);
        } catch (ParseException parseException) {
            LOG.error("Failed to parse Scope claim from token: {}", parseException.getMessage());
            throw new FailedLoginException();
        }
    }

    private void verifySignature(final SignedJWT jwt) throws FailedLoginException {
        final String kid = jwt.getHeader().getKeyID();
        if (kid == null || kid.isBlank()) {
            LOG.error("Token header had an empty Key ID");
            throw new FailedLoginException();
        }

        final Optional<PublicKey> publicKey = keyProvider.getKey(kid);
        if (publicKey.isEmpty()) {
            LOG.error("Could not find Public Key with ID {}", kid);
            throw new FailedLoginException();
        }

        try {
            final JWSVerifier verifier = getVerifier(publicKey.get());
            if (!jwt.verify(verifier)) {
                LOG.error("Token had an invalid signature");
                throw new FailedLoginException();
            }
        } catch (final JOSEException joseException) {
            LOG.error("There was a problem verifying the token signature: {}", joseException.getMessage());
            throw new FailedLoginException();
        }
    }

    private JWSVerifier getVerifier(final PublicKey key) throws JOSEException {
        final String algorithm = key.getAlgorithm();
        LOG.info("Key algorithm is {}", algorithm);

        if ("RSA".equals(algorithm)) {
            return new RSASSAVerifier((RSAPublicKey) key);
        }

        if ("EC".equals(algorithm)) {
            return new ECDSAVerifier((ECPublicKey) key);
        }

        throw new IllegalArgumentException(String.format("Key algorithm '%s' is not supported", algorithm));
    }

    private void verifyClaims(final JWTClaimsSet claims) throws FailedLoginException, CredentialExpiredException {
        final Date expirationTime = claims.getExpirationTime();
        if (expirationTime == null) {
            LOG.error("Expiration time not found in the token");
            throw new FailedLoginException();
        }

        if (expirationTime.before(new Date())) { // TODO: have a way to inject a specific time (useful for tests)
            LOG.error("Token is expired");
            throw new CredentialExpiredException();
        }

        final String tokenIssuer = claims.getIssuer();
        if (tokenIssuer == null) {
            LOG.error("Issuer claim not found in the token");
            throw new FailedLoginException();
        }

        if (!issuer.equals(tokenIssuer)) {
            LOG.error("Token issuer mismatch. Expected '{}', got '{}'", issuer, tokenIssuer);
            throw new FailedLoginException();
        }

        final boolean shouldValidateAudience = (audience != null && !audience.isEmpty()); // Expected audience is optional
        if (shouldValidateAudience) {
            final List<String> tokenAudiences = claims.getAudience();
            if (tokenAudiences == null || tokenAudiences.isEmpty()) {
                LOG.error("Audience claim not found in the token");
                throw new FailedLoginException();
            }

            if (tokenAudiences.stream().noneMatch(audience::equals)) {
                LOG.error("Token audience mismatch. Expected '{}', got '{}'", audience, tokenAudiences);
                throw new FailedLoginException();
            }
        }
    }
}
