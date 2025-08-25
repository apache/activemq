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

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;

import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.jaas.UserPrincipal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OAuthLoginModule implements LoginModule {
    private static final Logger LOG = LoggerFactory.getLogger(OAuthLoginModule.class);

    // TODO: Need a better way to inject a singleton JWKProvider, probably needs to be configurable
    private static JWKProvider jwkProvider;

    private boolean loginSucceeded = false;
    private boolean commitSucceeded = false;
    private final Set<GroupPrincipal> groups = new HashSet<>();
    private UserPrincipal userPrincipal;

    private OAuthAuthenticator authenticator;
    private GroupResolver groupResolver;

    // Shared across the Login Modules chain
    private Subject subject;
    private CallbackHandler callbackHandler;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState, Map<String, ?> options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        
        // Setup configurations

        final String issuer = (String) options.get("issuer");
        final String audience = (String) options.get("audience");
        final String maybeJwksUri = (String) options.get("jwks_uri");
        final String groupResolverClass = (String) options.get("group_resolver_class");

        validateConfigurations(issuer, audience, maybeJwksUri, groupResolverClass);

        URI jwksUri;
        try {
            final boolean hasJwksConfiguration = (maybeJwksUri != null && !maybeJwksUri.isBlank());
            if (hasJwksConfiguration) {
                jwksUri = new URI(maybeJwksUri);
            } else {
                jwksUri = getDefaultJwksUri(issuer);
            }
        } catch (URISyntaxException uriEx) {
            // TODO: handle malformed URLs in the configuration validation
            throw new RuntimeException(uriEx);
        }

        LOG.info("Initializing plugin with issuer='{}', audience='{}', jwksUri='{}'", issuer, audience, jwksUri);

        // Setup dependencies

        JWKProvider keyProvider;
        synchronized(OAuthLoginModule.class) { // TODO: Avoid serializing all calls to initialize() if the jwkProvider is already initialized
            keyProvider = getKeyProvider(jwksUri);
        }

        authenticator = new OAuthAuthenticatorImpl(keyProvider, issuer, audience);
        groupResolver = getGroupResolver(groupResolverClass);
    }

    private void validateConfigurations(final String issuer, final String audience, final String jwksUri, final String groupResolverClass) {
        // TODO: implement validation for the config values.
    }

    private URI getDefaultJwksUri(final String issuer) throws URISyntaxException {
        final URI issuerUrl = new URI(issuer);
        return issuerUrl.resolve(new URI(".well-known/jwks.json"));
    }

    private JWKProvider getKeyProvider(final URI jwksUri) {
        if (jwkProvider == null) {
            // TODO: This is not testable, need to inject in runtime. Do the same as for the GroupResolver.
            jwkProvider = new CachedJWKProvider(jwksUri);
        }
        return jwkProvider;
    }

    private GroupResolver getGroupResolver(final String className) {
        try {
            // TODO: Double check if this is the best way to inject the right implementation
            final Class<?> clazz = Thread.currentThread().getContextClassLoader().loadClass(className);
            return (GroupResolver) clazz.getDeclaredConstructor().newInstance();
        } catch(final Exception ex) {
            // TODO: Double check if IllegalStateException is the best exception to throw here
            LOG.error("Cannot load Group Resolver \"{}\": {}", className, ex.getMessage());
            throw new IllegalStateException("Cannot load Group Resolver", ex);
        }
    }

    @Override
    public boolean login() throws LoginException {
        LOG.info("Executing login()");

        final NameCallback usernameCallback = new NameCallback("username:");
        final PasswordCallback passwordCallback = new PasswordCallback("password:", false);

        try {
            callbackHandler.handle(new Callback[] {usernameCallback, passwordCallback});
        } catch (IOException | UnsupportedCallbackException e) {
            throw new LoginException("Error handling callbacks: " + e.getMessage());
        }

        final char[] password = passwordCallback.getPassword();
        if (password == null) {
            // TODO: maybe we should return false instead
            throw new FailedLoginException("No OAuth token provided");
        }

        final String token = new String(password);
        if (token.isBlank()) {
            // TODO: maybe we should return false instead
            throw new FailedLoginException("OAuth token is empty");
        }

        LOG.info("login(): token is '{}'", token);

        final AuthenticationResult result = authenticator.authenticate(token);
        LOG.info("Token successfully authenticated: {}", result);

        loginSucceeded = true;

        userPrincipal = new UserPrincipal(
                result.getUser().orElse(
                        usernameCallback.getName()));

        groups.addAll(
                groupResolver.getGroups(result));

        return true;
    }

    @Override
    public boolean commit() throws LoginException {
        LOG.info("Executing commit()");

        if (!loginSucceeded) {
            return false;
        }

        if (userPrincipal != null) {
            subject.getPrincipals().add(userPrincipal);
        }
        subject.getPrincipals().addAll(groups);

        commitSucceeded = true;
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        LOG.info("Executing logout()");

        if (userPrincipal != null) {
            subject.getPrincipals().remove(userPrincipal);
        }
        subject.getPrincipals().removeAll(groups);

        clear();
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        LOG.info("Executing abort()");

        if (!loginSucceeded) {
            return false;
        }

        if (commitSucceeded) {
            // we succeeded, but another required module failed
            logout();
        } else {
            // our commit failed
            clear();
        }

        return true;
    }

    private void clear() {
        loginSucceeded = false;
        commitSucceeded = false;
        userPrincipal = null;
        groups.clear();
    }
}
