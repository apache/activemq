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
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

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

    private static final Logger LOG = LoggerFactory.getLogger(PropertiesLoginModule.class);

    private Subject subject;
    private CallbackHandler callbackHandler;

    private Properties users;
    private Map<String,Set<String>> groups;
    private String user;
    private final Set<Principal> principals = new HashSet<Principal>();

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

        principals.add(new UserPrincipal(user));

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
        principals.clear();
    }

}
