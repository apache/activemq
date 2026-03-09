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
package org.apache.activemq.jaas;

import java.io.IOException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.interfaces.RSAPublicKey;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.JWSHeader;
import com.nimbusds.jose.JWSSigner;
import com.nimbusds.jose.crypto.RSASSASigner;
import com.nimbusds.jose.jwk.JWKSet;
import com.nimbusds.jose.jwk.RSAKey;
import com.nimbusds.jose.jwk.source.ImmutableJWKSet;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.SignedJWT;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

import junit.framework.TestCase;

public class OAuth2LoginModuleTest extends TestCase {

    private KeyPair keyPair;
    private RSAKey rsaKey;
    private JWSSigner signer;
    private ConfigurableJWTProcessor<SecurityContext> jwtProcessor;

    private static final String ISSUER = "https://idp.example.com";
    private static final String AUDIENCE = "activemq";
    private static final String KEY_ID = "test-key-1";

    @Override
    protected void setUp() throws Exception {
        super.setUp();

        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        keyPair = gen.generateKeyPair();

        rsaKey = new RSAKey.Builder((RSAPublicKey) keyPair.getPublic())
                .keyID(KEY_ID)
                .build();

        signer = new RSASSASigner(keyPair.getPrivate());

        // Build a JWT processor with an in-memory JWK source
        JWKSet jwkSet = new JWKSet(rsaKey);
        ImmutableJWKSet<SecurityContext> keySource = new ImmutableJWKSet<>(jwkSet);

        jwtProcessor = new DefaultJWTProcessor<>();
        jwtProcessor.setJWSKeySelector(
                new JWSVerificationKeySelector<>(JWSAlgorithm.Family.RSA, keySource));

        Set<String> requiredClaims = new HashSet<>();
        requiredClaims.add("sub");
        requiredClaims.add("iss");
        requiredClaims.add("exp");
        requiredClaims.add("aud");

        jwtProcessor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(
                new JWTClaimsSet.Builder()
                        .issuer(ISSUER)
                        .audience(AUDIENCE)
                        .build(),
                requiredClaims));
    }

    public void testSuccessfulLogin() throws Exception {
        String token = createToken("testuser", Arrays.asList("admin", "users"), new Date(System.currentTimeMillis() + 300000));

        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        assertTrue("Login should succeed", module.login());
        assertTrue("Commit should succeed", module.commit());

        Set<UserPrincipal> userPrincipals = subject.getPrincipals(UserPrincipal.class);
        Set<GroupPrincipal> groupPrincipals = subject.getPrincipals(GroupPrincipal.class);

        assertEquals("Should have one user principal", 1, userPrincipals.size());
        assertEquals("Username should be testuser", "testuser", userPrincipals.iterator().next().getName());
        assertEquals("Should have two group principals", 2, groupPrincipals.size());

        boolean hasAdmin = false, hasUsers = false;
        for (GroupPrincipal gp : groupPrincipals) {
            if ("admin".equals(gp.getName())) hasAdmin = true;
            if ("users".equals(gp.getName())) hasUsers = true;
        }
        assertTrue("Should have admin group", hasAdmin);
        assertTrue("Should have users group", hasUsers);
    }

    public void testSuccessfulLogout() throws Exception {
        String token = createToken("testuser", Arrays.asList("admin"), new Date(System.currentTimeMillis() + 300000));

        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        module.login();
        module.commit();

        assertFalse("Should have principals after login", subject.getPrincipals().isEmpty());

        module.logout();
        assertTrue("Should have no principals after logout", subject.getPrincipals().isEmpty());
    }

    public void testExpiredToken() throws Exception {
        String token = createToken("testuser", Arrays.asList("admin"), new Date(System.currentTimeMillis() - 300000));

        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        try {
            module.login();
            fail("Should have thrown FailedLoginException for expired token");
        } catch (FailedLoginException e) {
            assertTrue("Error should mention validation", e.getMessage().contains("JWT validation failed"));
        }
    }

    public void testWrongIssuer() throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("testuser")
                .issuer("https://wrong-issuer.com")
                .audience(AUDIENCE)
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        try {
            module.login();
            fail("Should have thrown FailedLoginException for wrong issuer");
        } catch (FailedLoginException e) {
            // expected
        }
    }

    public void testWrongAudience() throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("testuser")
                .issuer(ISSUER)
                .audience("wrong-audience")
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        try {
            module.login();
            fail("Should have thrown FailedLoginException for wrong audience");
        } catch (FailedLoginException e) {
            // expected
        }
    }

    public void testNoToken() throws Exception {
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(null, subject);
        try {
            module.login();
            fail("Should have thrown FailedLoginException for no token");
        } catch (FailedLoginException e) {
            assertTrue("Error should mention no token", e.getMessage().contains("No JWT token"));
        }
    }

    public void testEmptyToken() throws Exception {
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule("", subject);
        try {
            module.login();
            fail("Should have thrown FailedLoginException for empty token");
        } catch (FailedLoginException e) {
            assertTrue("Error should mention no token", e.getMessage().contains("No JWT token"));
        }
    }

    public void testInvalidTokenFormat() throws Exception {
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule("not-a-jwt-token", subject);
        try {
            module.login();
            fail("Should have thrown FailedLoginException for invalid token");
        } catch (FailedLoginException e) {
            // expected
        }
    }

    public void testCustomUsernameClaim() throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("sub-id-123")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .claim("preferred_username", "jdoe")
                .claim("groups", Arrays.asList("users"))
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);

        Map<String, String> options = new HashMap<>();
        options.put(OAuth2LoginModule.JWKS_URL_OPTION, "https://idp.example.com/.well-known/jwks.json");
        options.put(OAuth2LoginModule.ISSUER_OPTION, ISSUER);
        options.put(OAuth2LoginModule.AUDIENCE_OPTION, AUDIENCE);
        options.put(OAuth2LoginModule.USERNAME_CLAIM_OPTION, "preferred_username");
        options.put("debug", "true");

        Subject subject = new Subject();
        OAuth2LoginModule module = new OAuth2LoginModule();
        module.initialize(subject, new TokenCallbackHandler(token), new HashMap<>(), options);
        module.setJwtProcessor(jwtProcessor);

        assertTrue("Login should succeed", module.login());
        assertTrue("Commit should succeed", module.commit());

        Set<UserPrincipal> userPrincipals = subject.getPrincipals(UserPrincipal.class);
        assertEquals("Should have one user principal", 1, userPrincipals.size());
        assertEquals("Username should be jdoe", "jdoe", userPrincipals.iterator().next().getName());
    }

    public void testTokenWithNoGroups() throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("testuser")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        assertTrue("Login should succeed", module.login());
        assertTrue("Commit should succeed", module.commit());

        assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertEquals("Should have no group principals", 0, subject.getPrincipals(GroupPrincipal.class).size());
    }

    public void testAbortBeforeLogin() throws Exception {
        String token = createToken("testuser", Arrays.asList("admin"), new Date(System.currentTimeMillis() + 300000));
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        assertFalse("Abort should return false when login hasn't succeeded", module.abort());
    }

    public void testAbortAfterLogin() throws Exception {
        String token = createToken("testuser", Arrays.asList("admin"), new Date(System.currentTimeMillis() + 300000));
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        module.login();
        module.commit();
        assertTrue("Abort should return true after successful login", module.abort());

        assertTrue("Should have no principals after abort", subject.getPrincipals().isEmpty());
    }

    public void testMissingJwksUrl() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(OAuth2LoginModule.ISSUER_OPTION, ISSUER);

        Subject subject = new Subject();
        OAuth2LoginModule module = new OAuth2LoginModule();
        try {
            module.initialize(subject, new TokenCallbackHandler("token"), new HashMap<>(), options);
            fail("Should have thrown IllegalArgumentException for missing JWKS URL");
        } catch (IllegalArgumentException e) {
            assertTrue("Error should mention JWKS URL", e.getMessage().contains("JWKS URL"));
        }
    }

    public void testMissingIssuer() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put(OAuth2LoginModule.JWKS_URL_OPTION, "https://idp.example.com/.well-known/jwks.json");

        Subject subject = new Subject();
        OAuth2LoginModule module = new OAuth2LoginModule();
        try {
            module.initialize(subject, new TokenCallbackHandler("token"), new HashMap<>(), options);
            fail("Should have thrown IllegalArgumentException for missing issuer");
        } catch (IllegalArgumentException e) {
            assertTrue("Error should mention issuer", e.getMessage().contains("issuer"));
        }
    }

    public void testPasswordCallbackFallback() throws Exception {
        String token = createToken("testuser", Arrays.asList("admin"), new Date(System.currentTimeMillis() + 300000));

        // Use a callback handler that only supports PasswordCallback (not OAuth2TokenCallback)
        CallbackHandler handler = callbacks -> {
            for (Callback callback : callbacks) {
                if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(token.toCharArray());
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        };

        Map<String, String> options = new HashMap<>();
        options.put(OAuth2LoginModule.JWKS_URL_OPTION, "https://idp.example.com/.well-known/jwks.json");
        options.put(OAuth2LoginModule.ISSUER_OPTION, ISSUER);
        options.put(OAuth2LoginModule.AUDIENCE_OPTION, AUDIENCE);
        options.put("debug", "true");

        Subject subject = new Subject();
        OAuth2LoginModule module = new OAuth2LoginModule();
        module.initialize(subject, handler, new HashMap<>(), options);
        module.setJwtProcessor(jwtProcessor);

        assertTrue("Login should succeed via PasswordCallback fallback", module.login());
        module.commit();

        assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertEquals("testuser", subject.getPrincipals(UserPrincipal.class).iterator().next().getName());
    }

    public void testTokenSignedWithWrongKey() throws Exception {
        // Generate a different key pair
        KeyPairGenerator gen = KeyPairGenerator.getInstance("RSA");
        gen.initialize(2048);
        KeyPair wrongKeyPair = gen.generateKeyPair();
        JWSSigner wrongSigner = new RSASSASigner(wrongKeyPair.getPrivate());

        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("testuser")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS256)
                .keyID(KEY_ID)
                .build();
        SignedJWT signedJWT = new SignedJWT(header, claims);
        signedJWT.sign(wrongSigner);
        String token = signedJWT.serialize();

        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        try {
            module.login();
            fail("Should have thrown FailedLoginException for wrong signing key");
        } catch (FailedLoginException e) {
            // expected - signature verification should fail
        }
    }

    public void testTamperedToken() throws Exception {
        String token = createToken("testuser", Arrays.asList("admin"), new Date(System.currentTimeMillis() + 300000));

        // Tamper with the payload by changing a character
        String[] parts = token.split("\\.");
        assertEquals("JWT should have 3 parts", 3, parts.length);
        // Modify the payload part
        char[] payloadChars = parts[1].toCharArray();
        payloadChars[0] = payloadChars[0] == 'a' ? 'b' : 'a';
        String tamperedToken = parts[0] + "." + new String(payloadChars) + "." + parts[2];

        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(tamperedToken, subject);
        try {
            module.login();
            fail("Should have thrown FailedLoginException for tampered token");
        } catch (FailedLoginException e) {
            // expected - signature won't match tampered payload
        }
    }

    public void testGroupsAsCommaSeparatedString() throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("testuser")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .claim("groups", "admin,users,operators")
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        assertTrue("Login should succeed", module.login());
        assertTrue("Commit should succeed", module.commit());

        Set<GroupPrincipal> groupPrincipals = subject.getPrincipals(GroupPrincipal.class);
        assertEquals("Should have three group principals", 3, groupPrincipals.size());

        Set<String> groupNames = new HashSet<>();
        for (GroupPrincipal gp : groupPrincipals) {
            groupNames.add(gp.getName());
        }
        assertTrue("Should have admin group", groupNames.contains("admin"));
        assertTrue("Should have users group", groupNames.contains("users"));
        assertTrue("Should have operators group", groupNames.contains("operators"));
    }

    public void testCustomGroupsClaim() throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("testuser")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .claim("roles", Arrays.asList("developer", "tester"))
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);

        Map<String, String> options = new HashMap<>();
        options.put(OAuth2LoginModule.JWKS_URL_OPTION, "https://idp.example.com/.well-known/jwks.json");
        options.put(OAuth2LoginModule.ISSUER_OPTION, ISSUER);
        options.put(OAuth2LoginModule.AUDIENCE_OPTION, AUDIENCE);
        options.put(OAuth2LoginModule.GROUPS_CLAIM_OPTION, "roles");
        options.put("debug", "true");

        Subject subject = new Subject();
        OAuth2LoginModule module = new OAuth2LoginModule();
        module.initialize(subject, new TokenCallbackHandler(token), new HashMap<>(), options);
        module.setJwtProcessor(jwtProcessor);

        assertTrue("Login should succeed", module.login());
        assertTrue("Commit should succeed", module.commit());

        Set<GroupPrincipal> groupPrincipals = subject.getPrincipals(GroupPrincipal.class);
        assertEquals("Should have two group principals", 2, groupPrincipals.size());

        Set<String> groupNames = new HashSet<>();
        for (GroupPrincipal gp : groupPrincipals) {
            groupNames.add(gp.getName());
        }
        assertTrue("Should have developer group", groupNames.contains("developer"));
        assertTrue("Should have tester group", groupNames.contains("tester"));
    }

    public void testMissingUsernameClaim() throws Exception {
        // Token with no "sub" claim and no custom username claim set
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .claim("sub", "")
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        try {
            module.login();
            fail("Should have thrown FailedLoginException for empty username claim");
        } catch (FailedLoginException e) {
            assertTrue("Error should mention username claim", e.getMessage().contains("username claim"));
        }
    }

    public void testCustomUsernameClaimMissing() throws Exception {
        // Token has "sub" but not the custom claim "email"
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("testuser")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);

        Map<String, String> options = new HashMap<>();
        options.put(OAuth2LoginModule.JWKS_URL_OPTION, "https://idp.example.com/.well-known/jwks.json");
        options.put(OAuth2LoginModule.ISSUER_OPTION, ISSUER);
        options.put(OAuth2LoginModule.AUDIENCE_OPTION, AUDIENCE);
        options.put(OAuth2LoginModule.USERNAME_CLAIM_OPTION, "email");
        options.put("debug", "true");

        Subject subject = new Subject();
        OAuth2LoginModule module = new OAuth2LoginModule();
        module.initialize(subject, new TokenCallbackHandler(token), new HashMap<>(), options);
        module.setJwtProcessor(jwtProcessor);

        try {
            module.login();
            fail("Should have thrown FailedLoginException for missing email claim");
        } catch (FailedLoginException e) {
            assertTrue("Error should mention username claim", e.getMessage().contains("username claim"));
            assertTrue("Error should mention email", e.getMessage().contains("email"));
        }
    }

    public void testCommitWithoutLogin() throws Exception {
        String token = createToken("testuser", Arrays.asList("admin"), new Date(System.currentTimeMillis() + 300000));
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);

        // Commit without calling login first
        assertFalse("Commit should return false when login hasn't succeeded", module.commit());
        assertTrue("Should have no principals when commit fails", subject.getPrincipals().isEmpty());
    }

    public void testAbortAfterLoginBeforeCommit() throws Exception {
        String token = createToken("testuser", Arrays.asList("admin"), new Date(System.currentTimeMillis() + 300000));
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);

        module.login();
        // Abort after login but before commit
        assertTrue("Abort should return true after successful login", module.abort());
        assertTrue("Should have no principals after abort without commit", subject.getPrincipals().isEmpty());
    }

    public void testLoginLogoutRelogin() throws Exception {
        String token = createToken("testuser", Arrays.asList("admin", "users"), new Date(System.currentTimeMillis() + 300000));
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);

        // First login
        module.login();
        module.commit();
        assertEquals("Should have 3 principals after first login", 3, subject.getPrincipals().size());

        // Logout
        module.logout();
        assertTrue("Should have no principals after logout", subject.getPrincipals().isEmpty());

        // Re-login with a new module (same subject)
        String token2 = createToken("anotheruser", Arrays.asList("viewers"), new Date(System.currentTimeMillis() + 300000));
        OAuth2LoginModule module2 = createModule(token2, subject);
        module2.login();
        module2.commit();

        Set<UserPrincipal> userPrincipals = subject.getPrincipals(UserPrincipal.class);
        assertEquals("Should have one user principal after re-login", 1, userPrincipals.size());
        assertEquals("Username should be anotheruser", "anotheruser", userPrincipals.iterator().next().getName());

        Set<GroupPrincipal> groupPrincipals = subject.getPrincipals(GroupPrincipal.class);
        assertEquals("Should have one group principal after re-login", 1, groupPrincipals.size());
        assertEquals("Group should be viewers", "viewers", groupPrincipals.iterator().next().getName());
    }

    public void testNoAudienceConfigured() throws Exception {
        // Build a processor that doesn't require audience
        ConfigurableJWTProcessor<SecurityContext> noAudProcessor = new DefaultJWTProcessor<>();
        JWKSet jwkSet = new JWKSet(rsaKey);
        ImmutableJWKSet<SecurityContext> keySource = new ImmutableJWKSet<>(jwkSet);
        noAudProcessor.setJWSKeySelector(
                new JWSVerificationKeySelector<>(JWSAlgorithm.Family.RSA, keySource));

        Set<String> requiredClaims = new HashSet<>();
        requiredClaims.add("sub");
        requiredClaims.add("iss");
        requiredClaims.add("exp");
        noAudProcessor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(
                new JWTClaimsSet.Builder().issuer(ISSUER).build(),
                requiredClaims));

        // Token without audience
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("testuser")
                .issuer(ISSUER)
                .claim("groups", Arrays.asList("admin"))
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);

        // Configure module without audience
        Map<String, String> options = new HashMap<>();
        options.put(OAuth2LoginModule.JWKS_URL_OPTION, "https://idp.example.com/.well-known/jwks.json");
        options.put(OAuth2LoginModule.ISSUER_OPTION, ISSUER);
        options.put("debug", "true");

        Subject subject = new Subject();
        OAuth2LoginModule module = new OAuth2LoginModule();
        module.initialize(subject, new TokenCallbackHandler(token), new HashMap<>(), options);
        module.setJwtProcessor(noAudProcessor);

        assertTrue("Login should succeed without audience", module.login());
        module.commit();

        assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertEquals("Should have one group principal", 1, subject.getPrincipals(GroupPrincipal.class).size());
    }

    public void testMultipleAudiencesInTokenRejected() throws Exception {
        // DefaultJWTClaimsVerifier requires exact audience match, so a token with
        // multiple audiences should be rejected when only a single audience is configured
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("testuser")
                .issuer(ISSUER)
                .audience(Arrays.asList("activemq", "other-service"))
                .claim("groups", Arrays.asList("admin"))
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);

        try {
            module.login();
            fail("Should have thrown FailedLoginException for multiple audiences");
        } catch (FailedLoginException e) {
            // expected - exact audience match required
        }
    }

    public void testSingleGroupAsList() throws Exception {
        String token = createToken("testuser", Collections.singletonList("admin"), new Date(System.currentTimeMillis() + 300000));
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        module.login();
        module.commit();

        Set<GroupPrincipal> groupPrincipals = subject.getPrincipals(GroupPrincipal.class);
        assertEquals("Should have one group principal", 1, groupPrincipals.size());
        assertEquals("Group should be admin", "admin", groupPrincipals.iterator().next().getName());
    }

    public void testEmptyGroupsList() throws Exception {
        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("testuser")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .claim("groups", Collections.emptyList())
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        module.login();
        module.commit();

        assertEquals("Should have one user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertEquals("Should have no group principals for empty list", 0, subject.getPrincipals(GroupPrincipal.class).size());
    }

    public void testCallbackHandlerIOException() throws Exception {
        CallbackHandler handler = callbacks -> {
            throw new IOException("Connection refused");
        };

        Map<String, String> options = new HashMap<>();
        options.put(OAuth2LoginModule.JWKS_URL_OPTION, "https://idp.example.com/.well-known/jwks.json");
        options.put(OAuth2LoginModule.ISSUER_OPTION, ISSUER);
        options.put(OAuth2LoginModule.AUDIENCE_OPTION, AUDIENCE);

        Subject subject = new Subject();
        OAuth2LoginModule module = new OAuth2LoginModule();
        module.initialize(subject, handler, new HashMap<>(), options);
        module.setJwtProcessor(jwtProcessor);

        try {
            module.login();
            fail("Should have thrown LoginException for callback IOException");
        } catch (LoginException e) {
            assertTrue("Error should mention retrieving token", e.getMessage().contains("Error retrieving"));
        }
    }

    public void testDefaultClaimValues() throws Exception {
        // Initialize with no username or groups claim options (should use defaults)
        Map<String, String> options = new HashMap<>();
        options.put(OAuth2LoginModule.JWKS_URL_OPTION, "https://idp.example.com/.well-known/jwks.json");
        options.put(OAuth2LoginModule.ISSUER_OPTION, ISSUER);
        options.put(OAuth2LoginModule.AUDIENCE_OPTION, AUDIENCE);

        JWTClaimsSet claims = new JWTClaimsSet.Builder()
                .subject("defaultuser")
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .claim("groups", Arrays.asList("default-group"))
                .expirationTime(new Date(System.currentTimeMillis() + 300000))
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString())
                .build();

        String token = signToken(claims);
        Subject subject = new Subject();
        OAuth2LoginModule module = new OAuth2LoginModule();
        module.initialize(subject, new TokenCallbackHandler(token), new HashMap<>(), options);
        module.setJwtProcessor(jwtProcessor);

        module.login();
        module.commit();

        // Default usernameClaim is "sub", default groupsClaim is "groups"
        assertEquals("defaultuser", subject.getPrincipals(UserPrincipal.class).iterator().next().getName());
        assertEquals("default-group", subject.getPrincipals(GroupPrincipal.class).iterator().next().getName());
    }

    public void testTokenWithManyGroups() throws Exception {
        List<String> manyGroups = Arrays.asList("group1", "group2", "group3", "group4", "group5",
                "group6", "group7", "group8", "group9", "group10");

        String token = createToken("testuser", manyGroups, new Date(System.currentTimeMillis() + 300000));
        Subject subject = new Subject();
        OAuth2LoginModule module = createModule(token, subject);
        module.login();
        module.commit();

        assertEquals("Should have 10 group principals", 10, subject.getPrincipals(GroupPrincipal.class).size());
        assertEquals("Should have 1 user principal", 1, subject.getPrincipals(UserPrincipal.class).size());
        assertEquals("Should have 11 total principals", 11, subject.getPrincipals().size());
    }

    public void testOAuth2TokenCallback() {
        OAuth2TokenCallback callback = new OAuth2TokenCallback();
        assertNull("Token should be null initially", callback.getToken());

        callback.setToken("test-token-value");
        assertEquals("Token should be set", "test-token-value", callback.getToken());

        callback.setToken(null);
        assertNull("Token should be null after reset", callback.getToken());
    }

    // --- Helper methods ---

    private String createToken(String subject, List<String> groups, Date expiration) throws JOSEException {
        JWTClaimsSet.Builder builder = new JWTClaimsSet.Builder()
                .subject(subject)
                .issuer(ISSUER)
                .audience(AUDIENCE)
                .expirationTime(expiration)
                .issueTime(new Date())
                .jwtID(UUID.randomUUID().toString());

        if (groups != null) {
            builder.claim("groups", groups);
        }

        return signToken(builder.build());
    }

    private String signToken(JWTClaimsSet claims) throws JOSEException {
        JWSHeader header = new JWSHeader.Builder(JWSAlgorithm.RS256)
                .keyID(KEY_ID)
                .build();
        SignedJWT signedJWT = new SignedJWT(header, claims);
        signedJWT.sign(signer);
        return signedJWT.serialize();
    }

    private OAuth2LoginModule createModule(String token, Subject subject) {
        Map<String, String> options = new HashMap<>();
        options.put(OAuth2LoginModule.JWKS_URL_OPTION, "https://idp.example.com/.well-known/jwks.json");
        options.put(OAuth2LoginModule.ISSUER_OPTION, ISSUER);
        options.put(OAuth2LoginModule.AUDIENCE_OPTION, AUDIENCE);
        options.put("debug", "true");

        OAuth2LoginModule module = new OAuth2LoginModule();
        module.initialize(subject, new TokenCallbackHandler(token), new HashMap<>(), options);
        module.setJwtProcessor(jwtProcessor);
        return module;
    }

    private static class TokenCallbackHandler implements CallbackHandler {
        private final String token;

        TokenCallbackHandler(String token) {
            this.token = token;
        }

        @Override
        public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof OAuth2TokenCallback) {
                    ((OAuth2TokenCallback) callback).setToken(token);
                } else if (callback instanceof PasswordCallback) {
                    if (token != null) {
                        ((PasswordCallback) callback).setPassword(token.toCharArray());
                    }
                } else {
                    throw new UnsupportedCallbackException(callback);
                }
            }
        }
    }
}
