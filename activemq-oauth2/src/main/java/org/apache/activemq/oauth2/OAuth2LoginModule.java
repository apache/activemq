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
import java.net.MalformedURLException;
import java.net.URL;
import java.security.Principal;
import java.text.ParseException;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.jaas.UserPrincipal;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A JAAS LoginModule that authenticates users via OAuth2 JWT access tokens.
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
 *   <li>{@code debug} (optional) - Enable debug logging</li>
 * </ul>
 * <p>
 * Example login.config:
 * <pre>
 * activemq-oauth2 {
 *     org.apache.activemq.oauth2.OAuth2LoginModule required
 *         oauth2.jwksUrl="https://idp.example.com/.well-known/jwks.json"
 *         oauth2.issuer="https://idp.example.com"
 *         oauth2.audience="activemq"
 *         oauth2.usernameClaim="preferred_username"
 *         oauth2.groupsClaim="roles";
 * };
 * </pre>
 */
public class OAuth2LoginModule implements LoginModule {

    private static final Logger LOG = LoggerFactory.getLogger(OAuth2LoginModule.class);

    static final String JWKS_URL_OPTION = "oauth2.jwksUrl";
    static final String ISSUER_OPTION = "oauth2.issuer";
    static final String AUDIENCE_OPTION = "oauth2.audience";
    static final String USERNAME_CLAIM_OPTION = "oauth2.usernameClaim";
    static final String GROUPS_CLAIM_OPTION = "oauth2.groupsClaim";

    private static final String DEFAULT_USERNAME_CLAIM = "sub";
    private static final String DEFAULT_GROUPS_CLAIM = "groups";

    private Subject subject;
    private CallbackHandler callbackHandler;
    private boolean debug;

    private String jwksUrl;
    private String issuer;
    private String audience;
    private String usernameClaim;
    private String groupsClaim;

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
        this.debug = Boolean.parseBoolean((String) options.get("debug"));

        this.jwksUrl = (String) options.get(JWKS_URL_OPTION);
        if (jwksUrl == null || jwksUrl.isEmpty()) {
            throw new IllegalArgumentException("OAuth2 JWKS URL (" + JWKS_URL_OPTION + ") is required");
        }

        this.issuer = (String) options.get(ISSUER_OPTION);
        if (issuer == null || issuer.isEmpty()) {
            throw new IllegalArgumentException("OAuth2 issuer (" + ISSUER_OPTION + ") is required");
        }

        this.audience = (String) options.get(AUDIENCE_OPTION);

        String userClaim = (String) options.get(USERNAME_CLAIM_OPTION);
        this.usernameClaim = (userClaim != null && !userClaim.isEmpty()) ? userClaim : DEFAULT_USERNAME_CLAIM;

        String grpClaim = (String) options.get(GROUPS_CLAIM_OPTION);
        this.groupsClaim = (grpClaim != null && !grpClaim.isEmpty()) ? grpClaim : DEFAULT_GROUPS_CLAIM;

        if (debug) {
            LOG.debug("OAuth2LoginModule initialized with jwksUrl={}, issuer={}, audience={}, usernameClaim={}, groupsClaim={}",
                    jwksUrl, issuer, audience, usernameClaim, groupsClaim);
        }
    }

    @Override
    public boolean login() throws LoginException {
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

            principals.add(new UserPrincipal(user));

            List<String> groups = getGroupsFromClaims(claims);
            if (groups != null) {
                for (String group : groups) {
                    principals.add(new GroupPrincipal(group));
                }
            }

            succeeded = true;
            if (debug) {
                LOG.debug("OAuth2 login succeeded for user={} with groups={}", user, groups);
            }
        } catch (FailedLoginException e) {
            throw e;
        } catch (Exception e) {
            LoginException le = new FailedLoginException("JWT token validation failed: " + e.getMessage());
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
            throw new LoginException("Error retrieving OAuth2 token: " + e.getMessage());
        }

        // Fall back to PasswordCallback (token passed as password)
        try {
            PasswordCallback passwordCallback = new PasswordCallback("Token: ", false);
            callbackHandler.handle(new Callback[]{passwordCallback});
            char[] tokenChars = passwordCallback.getPassword();
            if (tokenChars != null) {
                return new String(tokenChars);
            }
        } catch (IOException | UnsupportedCallbackException e) {
            throw new LoginException("Error retrieving token from PasswordCallback: " + e.getMessage());
        }

        return null;
    }

    JWTClaimsSet validateToken(String token) throws LoginException {
        try {
            ConfigurableJWTProcessor<SecurityContext> processor = getJWTProcessor();
            return processor.process(token, null);
        } catch (ParseException e) {
            throw new FailedLoginException("Invalid JWT format: " + e.getMessage());
        } catch (BadJOSEException e) {
            throw new FailedLoginException("JWT validation failed: " + e.getMessage());
        } catch (JOSEException e) {
            throw new FailedLoginException("JWT processing error: " + e.getMessage());
        }
    }

    private ConfigurableJWTProcessor<SecurityContext> getJWTProcessor() throws LoginException {
        if (jwtProcessor != null) {
            return jwtProcessor;
        }

        try {
            URL jwksEndpoint = new URL(jwksUrl);
            JWKSource<SecurityContext> keySource = JWKSourceBuilder
                    .create(jwksEndpoint)
                    .retrying(true)
                    .build();

            JWSKeySelector<SecurityContext> keySelector = new JWSVerificationKeySelector<>(
                    JWSAlgorithm.Family.RSA, keySource);

            ConfigurableJWTProcessor<SecurityContext> processor = new DefaultJWTProcessor<>();
            processor.setJWSKeySelector(keySelector);

            // Build the claims verifier with issuer and optional audience
            JWTClaimsSet.Builder exactMatchBuilder = new JWTClaimsSet.Builder()
                    .issuer(issuer);

            Set<String> requiredClaims = new HashSet<>();
            requiredClaims.add("sub");
            requiredClaims.add("iss");
            requiredClaims.add("exp");

            if (audience != null && !audience.isEmpty()) {
                exactMatchBuilder.audience(audience);
                requiredClaims.add("aud");
            }

            processor.setJWTClaimsSetVerifier(new DefaultJWTClaimsVerifier<>(
                    exactMatchBuilder.build(),
                    requiredClaims));

            jwtProcessor = processor;
            return jwtProcessor;
        } catch (MalformedURLException e) {
            throw new LoginException("Invalid JWKS URL: " + jwksUrl);
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> getGroupsFromClaims(JWTClaimsSet claims) {
        try {
            Object groupsValue = claims.getClaim(groupsClaim);
            if (groupsValue instanceof List) {
                return (List<String>) groupsValue;
            } else if (groupsValue instanceof String) {
                return List.of(((String) groupsValue).split(","));
            }
        } catch (Exception e) {
            if (debug) {
                LOG.debug("Could not extract groups from claim '{}': {}", groupsClaim, e.getMessage());
            }
        }
        return null;
    }

    private void clear() {
        user = null;
        principals.clear();
    }

    // Visible for testing
    void setJwtProcessor(ConfigurableJWTProcessor<SecurityContext> jwtProcessor) {
        this.jwtProcessor = jwtProcessor;
    }
}
