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
import java.util.LinkedHashSet;
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

    private static final Logger LOG = LoggerFactory.getLogger(PropertiesLoginModule.class);

    private Subject subject;
    private CallbackHandler callbackHandler;

    private Properties users;
    private Map<String,Set<String>> groups;
    // Optional: userId -> comma-separated clientId patterns. Null when clientId
    // authentication is not configured (the CLIENTID_FILE_PROP_NAME option is absent).
    private Properties clientIds;
    private String user;
    private String clientId;
    // LinkedHashSet so principal insertion order is preserved when copied into the
    // Subject: UserPrincipal is always added first, then ClientIdPrincipal (when
    // clientId authentication is enabled), then group principals.
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
            String requestedClientId = getClientId();
            if (requestedClientId != null && !requestedClientId.isEmpty()) {
                if (!isClientIdAllowed(user, requestedClientId)) {
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

        // UserPrincipal is always added first; ClientIdPrincipal (when a clientId was
        // authenticated) is added second, ahead of any group principals.
        principals.add(new UserPrincipal(user));

        if (clientId != null) {
            principals.add(new ClientIdPrincipal(clientId));
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
        clientId = null;
        principals.clear();
    }

    private String getClientId() throws LoginException {
        ClientIdCallback clientIdCallback = new ClientIdCallback();
        try {
            callbackHandler.handle(new Callback[] {clientIdCallback});
        } catch (IOException ioe) {
            throw new LoginException(ioe.getMessage());
        } catch (UnsupportedCallbackException uce) {
            // callback handler does not supply a clientId; treat as none
            return null;
        }
        return clientIdCallback.getClientId();
    }

    private boolean isClientIdAllowed(String userId, String clientId) {
        String patterns = clientIds.getProperty(userId);
        if (patterns == null) {
            // fall back to the generic per-user rule, e.g. ${userId} = ${userId}-*
            patterns = clientIds.getProperty(USER_TOKEN);
        }
        if (patterns == null) {
            return false;
        }
        for (String pattern : patterns.split(",")) {
            pattern = pattern.trim();
            if (pattern.isEmpty()) {
                continue;
            }
            if (matches(pattern.replace(USER_TOKEN, userId), clientId)) {
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
