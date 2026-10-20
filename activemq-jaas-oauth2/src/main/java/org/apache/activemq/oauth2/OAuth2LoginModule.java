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
package org.apache.activemq.oauth2;

import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.security.Principal;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import com.nimbusds.jose.JOSEException;
import com.nimbusds.jose.JWSAlgorithm;
import com.nimbusds.jose.jwk.source.JWKSource;
import com.nimbusds.jose.jwk.source.JWKSourceBuilder;
import com.nimbusds.jose.proc.BadJOSEException;
import com.nimbusds.jose.proc.JWSKeySelector;
import com.nimbusds.jose.proc.JWSVerificationKeySelector;
import com.nimbusds.jose.proc.SecurityContext;
import com.nimbusds.jwt.JWTClaimsSet;
import com.nimbusds.jwt.proc.ConfigurableJWTProcessor;
import com.nimbusds.jwt.proc.DefaultJWTClaimsVerifier;
import com.nimbusds.jwt.proc.DefaultJWTProcessor;

import org.apache.activemq.annotation.Experimental;
import org.apache.activemq.jaas.ConnectionCallback;
import org.apache.activemq.jaas.ConnectionPrincipal;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.jaas.UserPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JAAS LoginModule that authenticates users via OAuth2 JWT access tokens.
 * <p>
 * <b>Experimental:</b> this module is new and still needs field testing. Its
 * configuration options and behavior may change or be removed between any release.
 * <p>
 * The client passes the JWT access token as the password in the connection info.
 * The module validates the token signature using the JWKS endpoint and verifies
 * standard claims (issuer, audience, expiration).
 * <p>
 * Configuration options (in login.config):
 * <ul>
 *   <li>{@code oauth2.jwksUrl} (required) - URL to the JWKS endpoint for signature verification</li>
 *   <li>{@code oauth2.issuer} (required) - Expected token issuer (iss claim)</li>
 *   <li>{@code oauth2.audience} (optional) - Expected token audience (aud claim)</li>
 *   <li>{@code oauth2.usernameClaim} (optional, default: "sub") - JWT claim to use as username</li>
 *   <li>{@code oauth2.groupsClaim} (optional, default: "groups") - JWT claim containing group memberships</li>
 *   <li>{@code oauth2.networkConnectionGroups} (optional) - Comma-separated list of groups whose members
 *       may register a network connection (broker bridge). When set, a connection that declares itself a
 *       network connection is rejected unless the user belongs to one of these groups, and a
 *       {@link ConnectionPrincipal} carrying that decision is added to the Subject so the broker can
 *       enforce it if the connection declares itself a network connection after authenticating.
 *       When not set, network connections are not restricted by this module.</li>
 *   <li>{@code debug} (optional) - Enable debug logging</li>
 * </ul>
 * <p>
 * Example login.config:
 * <pre>
 * activemq-jaas-oauth2 {
 *     org.apache.activemq.oauth2.OAuth2LoginModule required
 *         oauth2.jwksUrl="https://idp.example.com/.well-known/jwks.json"
 *         oauth2.issuer="https://idp.example.com"
 *         oauth2.audience="activemq"
 *         oauth2.usernameClaim="preferred_username"
 *         oauth2.groupsClaim="roles"
 *         oauth2.networkConnectionGroups="amq-network";
 * };
 * </pre>
 */
@Experimental("Tech Preview for OAuth2 JWT authentication")
public class OAuth2LoginModule implements LoginModule {

    private static final Logger LOG = LoggerFactory.getLogger(OAuth2LoginModule.class);

    static final String JWKS_URL_OPTION = "oauth2.jwksUrl";
    static final String ISSUER_OPTION = "oauth2.issuer";
    static final String AUDIENCE_OPTION = "oauth2.audience";
    static final String USERNAME_CLAIM_OPTION = "oauth2.usernameClaim";
    static final String GROUPS_CLAIM_OPTION = "oauth2.groupsClaim";
    static final String NETWORK_CONNECTION_GROUPS_OPTION = "oauth2.networkConnectionGroups";

    private static final String DEFAULT_USERNAME_CLAIM = "sub";
    private static final String DEFAULT_GROUPS_CLAIM = "groups";

    private static final int MAX_CACHED_PROCESSORS = 100;

    private static final Map<String, ConfigurableJWTProcessor<SecurityContext>> PROCESSOR_CACHE = new ConcurrentHashMap<>();

    private Subject subject;
    private CallbackHandler callbackHandler;
    private boolean debug;

    private String jwksUrl;
    private String issuer;
    private String audience;
    private String usernameClaim;
    private String groupsClaim;
    private Set<String> networkConnectionGroups;
    private ConnectionCallback connection;
    private boolean networkConnectionAllowed;

    private String user;
    private final LinkedHashSet<Principal> principals = new LinkedHashSet<>();
    private boolean succeeded;
    private boolean commitSucceeded;

    private ConfigurableJWTProcessor<SecurityContext> jwtProcessor;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        this.succeeded = false;
        this.debug = Boolean.parseBoolean(String.valueOf(options.get("debug")));

        this.jwksUrl = option(options, JWKS_URL_OPTION);
        if (jwksUrl == null || jwksUrl.isEmpty()) {
            throw new IllegalArgumentException("OAuth2 JWKS URL (" + JWKS_URL_OPTION + ") is required");
        }

        this.issuer = option(options, ISSUER_OPTION);
        if (issuer == null || issuer.isEmpty()) {
            throw new IllegalArgumentException("OAuth2 issuer (" + ISSUER_OPTION + ") is required");
        }

        this.audience = option(options, AUDIENCE_OPTION);

        String userClaim = option(options, USERNAME_CLAIM_OPTION);
        this.usernameClaim = (userClaim != null && !userClaim.isEmpty()) ? userClaim : DEFAULT_USERNAME_CLAIM;

        String grpClaim = option(options, GROUPS_CLAIM_OPTION);
        this.groupsClaim = (grpClaim != null && !grpClaim.isEmpty()) ? grpClaim : DEFAULT_GROUPS_CLAIM;

        String networkGroups = option(options, NETWORK_CONNECTION_GROUPS_OPTION);
        if (networkGroups != null && !networkGroups.trim().isEmpty()) {
            this.networkConnectionGroups = new HashSet<>();
            for (String group : networkGroups.split(",")) {
                String trimmed = group.trim();
                if (!trimmed.isEmpty()) {
                    this.networkConnectionGroups.add(trimmed);
                }
            }
        } else {
            this.networkConnectionGroups = null;
        }

        if (debug) {
            LOG.debug("OAuth2LoginModule initialized with jwksUrl={}, issuer={}, audience={}, usernameClaim={}, groupsClaim={}",
                    jwksUrl, issuer, audience, usernameClaim, groupsClaim);
        }
    }

    private static String option(Map<String, ?> options, String key) {
        Object value = options.get(key);
        return value == null ? null : value.toString();
    }

    @Override
    public boolean login() throws LoginException {
        clear();
        succeeded = false;
        String token = getToken();
        if (token == null || token.isEmpty()) {
            throw new FailedLoginException("No JWT token provided");
        }

        try {
            JWTClaimsSet claims = validateToken(token);
            user = claims.getStringClaim(usernameClaim);
            if (user == null || user.isEmpty()) {
                throw new FailedLoginException("JWT token does not contain the username claim: " + usernameClaim);
            }

            List<String> groups = getGroupsFromClaims(claims);

            // When enabled, a network connection is denied unless the user belongs to one of the
            // configured groups. Application connections are unaffected.
            if (networkConnectionGroups != null) {
                connection = getConnectionCallback();
                networkConnectionAllowed = groups != null && !Collections.disjoint(groups, networkConnectionGroups);
                if (connection.isNetworkConnection() && !networkConnectionAllowed) {
                    throw new FailedLoginException("network connection is not allowed for user");
                }
            }

            principals.add(new UserPrincipal(user));

            if (connection != null) {
                principals.add(new ConnectionPrincipal(connection.getConnectionId(), connection.getClientId(),
                        networkConnectionAllowed, connection.isSsl(), connection.getRemoteAddress(),
                        connection.getTransportConnectorName()));
            }

            if (groups != null) {
                for (String group : groups) {
                    principals.add(new GroupPrincipal(group));
                }
            }

            succeeded = true;
            if (debug) {
                LOG.debug("OAuth2 login succeeded for user={}", user);
            }
        } catch (FailedLoginException e) {
            throw e;
        } catch (Exception e) {
            LOG.warn("OAuth2 login failed: {}", e.getMessage());
            clear();
            LoginException le = new FailedLoginException("JWT token validation failed");
            le.initCause(e);
            throw le;
        }

        return succeeded;
    }

    @Override
    public boolean commit() throws LoginException {
        if (!succeeded) {
            clear();
            if (debug) {
                LOG.debug("commit, result: false");
            }
            return false;
        }

        subject.getPrincipals().addAll(principals);
        commitSucceeded = true;

        if (debug) {
            LOG.debug("commit, result: true");
        }
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        if (debug) {
            LOG.debug("abort");
        }
        if (!succeeded) {
            return false;
        } else if (commitSucceeded) {
            logout();
        } else {
            clear();
            succeeded = false;
        }
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        subject.getPrincipals().removeAll(principals);
        clear();
        if (debug) {
            LOG.debug("logout");
        }
        succeeded = false;
        commitSucceeded = false;
        return true;
    }

    private ConnectionCallback getConnectionCallback() throws LoginException {
        ConnectionCallback connectionCallback = new ConnectionCallback();
        try {
            callbackHandler.handle(new Callback[]{connectionCallback});
        } catch (IOException e) {
            LoginException le = new LoginException("Error retrieving connection details: " + e.getMessage());
            le.initCause(e);
            throw le;
        } catch (UnsupportedCallbackException e) {
            // callback handler does not describe the connection; treat as an application connection
        }
        return connectionCallback;
    }

    private String getToken() throws LoginException {
        // Try OAuth2TokenCallback first, then fall back to PasswordCallback
        try {
            OAuth2TokenCallback tokenCallback = new OAuth2TokenCallback();
            callbackHandler.handle(new Callback[]{tokenCallback});
            if (tokenCallback.getToken() != null) {
                return tokenCallback.getToken();
            }
        } catch (UnsupportedCallbackException e) {
            // OAuth2TokenCallback not supported, fall back to PasswordCallback
            if (debug) {
                LOG.debug("OAuth2TokenCallback not supported, falling back to PasswordCallback");
            }
        } catch (IOException e) {
            LoginException le = new LoginException("Error retrieving OAuth2 token: " + e.getMessage());
            le.initCause(e);
            throw le;
        }

        // Fall back to PasswordCallback (token passed as password)
        PasswordCallback passwordCallback = new PasswordCallback("Token: ", false);
        try {
            callbackHandler.handle(new Callback[]{passwordCallback});
            char[] tokenChars = passwordCallback.getPassword();
            if (tokenChars != null) {
                return new String(tokenChars);
            }
        } catch (IOException | UnsupportedCallbackException e) {
            LoginException le = new LoginException("Error retrieving token from PasswordCallback: " + e.getMessage());
            le.initCause(e);
            throw le;
        } finally {
            passwordCallback.clearPassword();
        }

        return null;
    }

    JWTClaimsSet validateToken(String token) throws LoginException {
        try {
            ConfigurableJWTProcessor<SecurityContext> processor = getJWTProcessor();
            return processor.process(token, null);
        } catch (ParseException e) {
            LOG.warn("Invalid JWT format: {}", e.getMessage());
            throw new FailedLoginException("Invalid JWT format");
        } catch (BadJOSEException e) {
            LOG.warn("JWT validation failed: {}", e.getMessage());
            throw new FailedLoginException("JWT validation failed");
        } catch (JOSEException e) {
            LOG.warn("JWT processing error: {}", e.getMessage());
            throw new FailedLoginException("JWT processing error");
        }
    }

    private ConfigurableJWTProcessor<SecurityContext> getJWTProcessor() throws LoginException {
        if (jwtProcessor != null) {
            return jwtProcessor;
        }

        URL jwksEndpoint;
        try {
            jwksEndpoint = URI.create(jwksUrl).toURL();
        } catch (IllegalArgumentException | java.net.MalformedURLException e) {
            throw new LoginException("Invalid JWKS URL: " + jwksUrl);
        }
        if (!"https".equalsIgnoreCase(jwksEndpoint.getProtocol())) {
            throw new LoginException("JWKS URL must use https: " + jwksUrl);
        }

        String cacheKey = jwksUrl + "|" + issuer + "|" + (audience != null ? audience : "") + "|" + usernameClaim;
        if (PROCESSOR_CACHE.size() >= MAX_CACHED_PROCESSORS && !PROCESSOR_CACHE.containsKey(cacheKey)) {
            PROCESSOR_CACHE.clear();
        }
        return PROCESSOR_CACHE.computeIfAbsent(cacheKey, k -> createProcessor(jwksEndpoint));
    }

    private ConfigurableJWTProcessor<SecurityContext> createProcessor(URL jwksEndpoint) {
        JWKSource<SecurityContext> keySource = JWKSourceBuilder
                .create(jwksEndpoint)
                .retrying(true)
                .build();

        // Only asymmetric algorithms: never accept HMAC-signed tokens
        Set<JWSAlgorithm> algorithms = new HashSet<>();
        algorithms.addAll(JWSAlgorithm.Family.RSA);
        algorithms.addAll(JWSAlgorithm.Family.EC);
        algorithms.add(JWSAlgorithm.EdDSA);
        JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(algorithms, keySource);

        ConfigurableJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
        processor.setJWSKeySelector(keySelector);

        JWTClaimsSet.Builder exactMatchBuilder = new JWTClaimsSet.Builder().issuer(issuer);

        Set<String> requiredClaims = new HashSet<>();
        requiredClaims.add(usernameClaim);
        requiredClaims.add("iss");
        requiredClaims.add("exp");

        if (audience != null && !audience.isEmpty()) {
            exactMatchBuilder.audience(audience);
            requiredClaims.add("aud");
        }

        processor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(exactMatchBuilder.build(), requiredClaims));
        return processor;
    }

    private List<String> getGroupsFromClaims(JWTClaimsSet claims) {
        Object groupsValue = resolveClaim(claims, groupsClaim);
        if (groupsValue instanceof Collection<?>) {
            List<String> groups = new ArrayList<>();
            for (Object item : (Collection<?>) groupsValue) {
                if (item != null && !item.toString().trim().isEmpty()) {
                    groups.add(item.toString().trim());
                }
            }
            return groups;
        } else if (groupsValue instanceof String) {
            return Arrays.stream(((String) groupsValue).split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .toList();
        } else if (groupsValue != null) {
            LOG.warn("Ignoring groups claim '{}': unsupported type {}", groupsClaim, groupsValue.getClass().getName());
        }
        return Collections.emptyList();
    }

    /**
     * Resolves a claim by exact name first, then as a dotted path into nested JSON objects
     * (e.g. {@code realm_access.roles}).
     */
    private static Object resolveClaim(JWTClaimsSet claims, String name) {
        Object value = claims.getClaim(name);
        if (value != null || name.indexOf('.') < 0) {
            return value;
        }
        String[] parts = name.split("\\.");
        Object current = claims.getClaim(parts[0]);
        for (int i = 1; i < parts.length && current != null; i++) {
            current = current instanceof Map<?, ?> ? ((Map<?, ?>) current).get(parts[i]) : null;
        }
        return current;
    }

    private void clear() {
        user = null;
        connection = null;
        networkConnectionAllowed = false;
        principals.clear();
    }

    // Visible for testing
    void setJwtProcessor(ConfigurableJWTProcessor<SecurityContext> jwtProcessor) {
        this.jwtProcessor = jwtProcessor;
    }

    // Visible for testing
    static void clearProcessorCache() {
        PROCESSOR_CACHE.clear();
    }
}
