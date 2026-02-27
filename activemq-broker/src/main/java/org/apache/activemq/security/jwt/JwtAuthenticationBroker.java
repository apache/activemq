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
package org.apache.activemq.security.jwt;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.security.AbstractAuthenticationBroker;
import org.apache.activemq.security.SecurityContext;
import org.jose4j.jwa.AlgorithmConstraints;
import org.jose4j.jws.AlgorithmIdentifiers;
import org.jose4j.jwt.JwtClaims;
import org.jose4j.jwt.MalformedClaimException;
import org.jose4j.jwt.consumer.InvalidJwtException;
import org.jose4j.jwt.consumer.JwtConsumer;
import org.jose4j.jwt.consumer.JwtConsumerBuilder;
import org.jose4j.jwt.consumer.JwtContext;

import java.security.Key;
import java.security.KeyFactory;
import java.security.NoSuchAlgorithmException;
import java.security.Principal;
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.X509EncodedKeySpec;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Handle authentication an user based on a JWT token
 */
public class JwtAuthenticationBroker extends AbstractAuthenticationBroker {

    private final String jwtIssuer;
    private final Claims jwtGroupsClaim;
    private final String jwtValidatingPublicKey;

    public JwtAuthenticationBroker(
            final Broker next,
            final String jwtIssuer,
            final Claims jwtGroupsClaim,
            final String jwtValidatingPublicKey) {
        super(next);
        this.jwtIssuer = jwtIssuer;
        this.jwtGroupsClaim = jwtGroupsClaim;
        this.jwtValidatingPublicKey = jwtValidatingPublicKey;
    }

    @Override
    public void addConnection(final ConnectionContext context, final ConnectionInfo info) throws Exception {
        SecurityContext securityContext = context.getSecurityContext();
        if (securityContext == null) {
            securityContext = authenticate(info.getUserName(), info.getPassword(), null);
            context.setSecurityContext(securityContext);
            securityContexts.add(securityContext);
        }

        try {
            super.addConnection(context, info);
        } catch (Exception e) {
            securityContexts.remove(securityContext);
            context.setSecurityContext(null);
            throw e;
        }
    }

    @Override
    public SecurityContext authenticate(final String username, final String password, final X509Certificate[] certificates) throws SecurityException {
        SecurityContext securityContext = null;
        if (!username.isEmpty()) {

            // parse the JWT token and check signature, validity, nbf
            try {
                final JwtConsumerBuilder builder = new JwtConsumerBuilder()
                        .setRelaxVerificationKeyValidation()
                        .setRequireSubject()
                        .setSkipDefaultAudienceValidation()
                        .setRequireExpirationTime()
                        .setExpectedIssuer(jwtIssuer)
                        .setAllowedClockSkewInSeconds(5)
                        .setVerificationKey(parsePCKS8(jwtValidatingPublicKey))
                        .setJwsAlgorithmConstraints(
                                new AlgorithmConstraints(AlgorithmConstraints.ConstraintType.WHITELIST,
                                        AlgorithmIdentifiers.RSA_USING_SHA256,
                                        AlgorithmIdentifiers.RSA_USING_SHA384,
                                        AlgorithmIdentifiers.RSA_USING_SHA512)
                        );

                final JwtConsumer jwtConsumer = builder.build();
                final JwtContext jwtContext = jwtConsumer.process(username);

                // validate the JWT and process it to the claims
                jwtConsumer.processContext(jwtContext);
                final JwtClaims claimsSet = jwtContext.getJwtClaims();

                // we have to determine the unique name to use as the principal name. It comes from upn, preferred_username, sub in that order
                String principalName = claimsSet.getClaimValue(Claims.upn.name(), String.class);
                if (principalName == null) {
                    principalName = claimsSet.getClaimValue(Claims.preferred_username.name(), String.class);
                    if (principalName == null) {
                        principalName = claimsSet.getSubject();
                    }
                }

                final Set<String> groups = new HashSet<>();
                final List<String> globalGroups = claimsSet.getStringListClaimValue(jwtGroupsClaim.name());
                if (globalGroups != null) {
                    groups.addAll(globalGroups);
                }

                securityContext = new SecurityContext(principalName) {
                    @Override
                    public Set<Principal> getPrincipals() {
                        return groups.stream().map(GroupPrincipal::new).collect(Collectors.toSet());
                    }
                };
            } catch (final InvalidJwtException e) {
                throw new RuntimeException("Failed to verify token", e);
            } catch (final MalformedClaimException e) {
                throw new RuntimeException("Failed to verify token claims", e);
            }
        } else {
            // login as anonymous without any group or fail
            // or whatever logic you should apply when no credentials are available
            securityContext = new SecurityContext("anonymous") {
                @Override
                public Set<Principal> getPrincipals() {
                    return Collections.emptySet();
                }
            };
        }

        return securityContext;
    }

    private Key parsePCKS8(final String publicKey) {
        try {
            final X509EncodedKeySpec spec = new X509EncodedKeySpec(normalizeAndDecodePCKS8(publicKey));
            final KeyFactory kf = KeyFactory.getInstance("RSA");
            return kf.generatePublic(spec);
        } catch (final NoSuchAlgorithmException | InvalidKeySpecException | IllegalArgumentException e) {
            return null;
        }
    }

    private byte[] normalizeAndDecodePCKS8(final String publicKey) {
        if (publicKey.contains("PRIVATE KEY")) {
            throw new RuntimeException("Public Key is Private.");
        }

        final String normalizedKey =
                publicKey.replaceAll("-----BEGIN (.*)-----", "")
                        .replaceAll("-----END (.*)----", "")
                        .replaceAll("\r\n", "")
                        .replaceAll("\n", "");

        return Base64.getDecoder().decode(normalizedKey);
    }

}
