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
import java.security.Principal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PropertiesLoginModule extends PropertiesLoader implements LoginModule {

    private static final String USER_FILE_PROP_NAME = "org.apache.activemq.jaas.properties.user";
    private static final String GROUP_FILE_PROP_NAME = "org.apache.activemq.jaas.properties.group";
    private static final String CLIENTID_FILE_PROP_NAME = "org.apache.activemq.jaas.properties.clientid";

    /** matches the authenticated user name when expanded in a clientId pattern */
    private static final String USER_TOKEN = "${userId}";
    /** matches the name of the broker performing the authentication when expanded in a clientId pattern */
    private static final String BROKER_TOKEN = "${brokerName}";

    /**
     * A parsed clientids.properties entry:
     * {@code <userId> = true|false, <clientId pattern>, <clientId pattern>...}
     * The leading boolean states whether the user may register a network connection.
     */
    private static final class ClientIdRule {
        final boolean networkConnectionAllowed;
        final List<String> patterns;

        ClientIdRule(boolean networkConnectionAllowed, List<String> patterns) {
            this.networkConnectionAllowed = networkConnectionAllowed;
            this.patterns = patterns;
        }
    }

    private static final Logger LOG = LoggerFactory.getLogger(PropertiesLoginModule.class);

    private Subject subject;
    private CallbackHandler callbackHandler;

    private Properties users;
    private Map<String,Set<String>> groups;
    // Optional: userId -> "true|false, clientId patterns". Null when clientId
    // authentication is not configured (the CLIENTID_FILE_PROP_NAME option is absent).
    private Properties clientIds;
    private String user;
    // the connection being authenticated, as described by the callback handler
    private ConnectionCallback connection;
    private String clientId;
    private boolean networkConnectionAllowed;
    // LinkedHashSet so principal insertion order is preserved when copied into the
    // Subject: UserPrincipal is always added first, then ConnectionPrincipal (when
    // clientId authorization is enabled), then group principals.
    private final Set<Principal> principals = new LinkedHashSet<Principal>();

    /** the authentication status*/
    private boolean succeeded = false;
    private boolean commitSucceeded = false;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        succeeded = false;
        init(options);
        users = load(USER_FILE_PROP_NAME, "user", options).getProps();
        groups = load(GROUP_FILE_PROP_NAME, "group", options).invertedPropertiesValuesMap();
        // clientId authentication is opt-in: only enabled when the file option is present
        if (options.containsKey(CLIENTID_FILE_PROP_NAME)) {
            clientIds = load(CLIENTID_FILE_PROP_NAME, "clientids", options).getProps();
        }
    }

    @Override
    public boolean login() throws LoginException {
        Callback[] callbacks = new Callback[2];

        callbacks[0] = new NameCallback("Username: ");
        callbacks[1] = new PasswordCallback("Password: ", false);
        try {
            callbackHandler.handle(callbacks);
        } catch (IOException ioe) {
            throw new LoginException(ioe.getMessage());
        } catch (UnsupportedCallbackException uce) {
            throw new LoginException(uce.getMessage() + " not available to obtain information from user");
        }
        user = ((NameCallback) callbacks[0]).getName();
        char[] tmpPassword = ((PasswordCallback) callbacks[1]).getPassword();
        if (tmpPassword == null) {
            tmpPassword = new char[0];
        }
        if (user == null) {
            throw new FailedLoginException("user name is null");
        }
        String password = users.getProperty(user);

        if (password == null) {
            throw new FailedLoginException("User does exist");
        }
        if (!password.equals(new String(tmpPassword))) {
            throw new FailedLoginException("Password does not match");
        }

        // When enabled, also authenticate the connection's clientId. A connection
        // that presents a clientId it is not permitted to use fails to log in. A
        // connection with no clientId is allowed (it cannot own durable subscriptions).
        if (clientIds != null) {
            connection = getConnectionCallback();
            ClientIdRule rule = ruleFor(user);
            // A network connection is denied unless the administrator has explicitly
            // allowed this user to register one. Application connections are unaffected.
            networkConnectionAllowed = rule != null && rule.networkConnectionAllowed;
            if (connection.isNetworkConnection() && !networkConnectionAllowed) {
                throw new FailedLoginException("network connection is not allowed for user");
            }
            String requestedClientId = connection.getClientId();
            if (requestedClientId != null && !requestedClientId.isEmpty()) {
                if (rule == null || !isClientIdAllowed(rule, user, requestedClientId, connection.getBrokerName())) {
                    throw new FailedLoginException("clientId is not allowed for user");
                }
                clientId = requestedClientId;
            }
        }

        succeeded = true;

        if (debug) {
            LOG.debug("login " + user);
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

        // UserPrincipal is always added first; ConnectionPrincipal (when clientId
        // authorization is enabled) is added second, ahead of any group principals. It
        // carries the allowed clientId and the network connection decision so the broker
        // can enforce the latter if the connection declares itself a network connection
        // after authenticating.
        principals.add(new UserPrincipal(user));

        if (clientIds != null) {
            principals.add(new ConnectionPrincipal(connection.getConnectionId(), clientId, networkConnectionAllowed,
                    connection.isSsl(), connection.getRemoteAddress(), connection.getTransportConnectorName()));
        }

        Set<String> matchedGroups = groups.get(user);
        if (matchedGroups != null) {
            for (String entry : matchedGroups) {
                principals.add(new GroupPrincipal(entry));
            }
        }

        subject.getPrincipals().addAll(principals);

        if (debug) {
            LOG.debug("commit, result: true");
        }

        commitSucceeded = true;
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        if (debug) {
            LOG.debug("abort");
        }
        if (!succeeded) {
            return false;
        } else if (succeeded && commitSucceeded) {
            // we succeeded, but another required module failed
            logout();
        } else {
            // our commit failed
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

    private void clear() {
        user = null;
        connection = null;
        clientId = null;
        networkConnectionAllowed = false;
        principals.clear();
    }

    private ConnectionCallback getConnectionCallback() throws LoginException {
        ConnectionCallback connectionCallback = new ConnectionCallback();
        try {
            callbackHandler.handle(new Callback[] {connectionCallback});
        } catch (IOException ioe) {
            throw new LoginException(ioe.getMessage());
        } catch (UnsupportedCallbackException uce) {
            // callback handler does not describe the connection; treat as an
            // application connection with no clientId
        }
        return connectionCallback;
    }

    /**
     * The rule for a user: their own entry, or the {@code ${userId}} fallback when
     * they have none. An explicit entry that fails to parse denies everything for
     * that user rather than falling back.
     */
    private ClientIdRule ruleFor(String userId) {
        String value = clientIds.getProperty(userId);
        if (value != null) {
            return parseRule(userId, value);
        }
        value = clientIds.getProperty(USER_TOKEN);
        return value != null ? parseRule(USER_TOKEN, value) : null;
    }

    /**
     * Parses {@code true|false, pattern, pattern...}. The boolean is required so
     * that permission to register a network connection is always stated.
     */
    private static ClientIdRule parseRule(String key, String value) {
        String[] tokens = value.split(",");
        String first = tokens[0].trim();
        if (!"true".equalsIgnoreCase(first) && !"false".equalsIgnoreCase(first)) {
            LOG.warn("Ignoring clientId rule for '{}': the first value must be true or false (may register a network connection), found '{}'",
                    key, first);
            return null;
        }
        List<String> patterns = new ArrayList<String>();
        for (int i = 1; i < tokens.length; i++) {
            String pattern = tokens[i].trim();
            if (!pattern.isEmpty()) {
                patterns.add(pattern);
            }
        }
        return new ClientIdRule(Boolean.parseBoolean(first), patterns);
    }

    private static boolean isClientIdAllowed(ClientIdRule rule, String userId, String clientId, String brokerName) {
        for (String pattern : rule.patterns) {
            pattern = pattern.replace(USER_TOKEN, userId);
            if (brokerName != null) {
                pattern = pattern.replace(BROKER_TOKEN, brokerName);
            }
            if (matches(pattern, clientId)) {
                return true;
            }
        }
        return false;
    }

    private static boolean matches(String pattern, String clientId) {
        // '*' is a multi-character wildcard; all other characters match literally.
        StringBuilder regex = new StringBuilder();
        String[] segments = pattern.split("\\*", -1);
        for (int i = 0; i < segments.length; i++) {
            if (i > 0) {
                regex.append(".*");
            }
            regex.append(Pattern.quote(segments[i]));
        }
        return clientId.matches(regex.toString());
    }

}
