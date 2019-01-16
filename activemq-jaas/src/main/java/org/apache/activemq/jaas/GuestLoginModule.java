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
import java.util.Set;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Always login the user with a default 'guest' identity.
 *
 * Useful for unauthenticated communication channels being used in the
 * same broker as authenticated ones.
 * 
 */
public class GuestLoginModule implements LoginModule {

    private static final String GUEST_USER = "org.apache.activemq.jaas.guest.user";
    private static final String GUEST_GROUP = "org.apache.activemq.jaas.guest.group";

    private static final Logger LOG = LoggerFactory.getLogger(GuestLoginModule.class);
    

    private String userName = "guest";
    private String groupName = "guests";
    private Subject subject;
    private boolean debug;
    private boolean credentialsInvalidate;
    private Set<Principal> principals = new HashSet<Principal>();
    private CallbackHandler callbackHandler;

    /** the authentication status*/
    private boolean succeeded = false;
    private boolean commitSucceeded = false;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        debug = "true".equalsIgnoreCase((String)options.get("debug"));
        credentialsInvalidate = "true".equalsIgnoreCase((String)options.get("credentialsInvalidate"));
        if (options.get(GUEST_USER) != null) {
            userName = (String)options.get(GUEST_USER);
        }
        if (options.get(GUEST_GROUP) != null) {
            groupName = (String)options.get(GUEST_GROUP);
        }
        principals.add(new UserPrincipal(userName));
        principals.add(new GroupPrincipal(groupName));
        
        if (debug) {
            LOG.debug("Initialized debug=" + debug + " guestUser=" + userName + " guestGroup=" + groupName);
        }

    }

    @Override
    public boolean login() throws LoginException {
        succeeded = true;
        if (credentialsInvalidate) {
            PasswordCallback passwordCallback = new PasswordCallback("Password: ", false);
            try {
                 callbackHandler.handle(new Callback[]{passwordCallback});
                 if (passwordCallback.getPassword() != null) {
                     if (debug) {
                        LOG.debug("Guest login failing (credentialsInvalidate=true) on presence of a password");
                     }
                     succeeded = false;
                     passwordCallback.clearPassword();
                 };
             } catch (IOException ioe) {
             } catch (UnsupportedCallbackException uce) {
             }
        }
        if (debug) {
            LOG.debug("Guest login " + succeeded);
        }
        return succeeded;
    }

    @Override
    public boolean commit() throws LoginException {
        if (debug) {
            LOG.debug("commit");
        }

        if (!succeeded) {
            return false;
        }

        subject.getPrincipals().addAll(principals);
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
            succeeded = false;
        }
        return true;
    }

    @Override
    public boolean logout() throws LoginException {
        subject.getPrincipals().removeAll(principals);

        if (debug) {
            LOG.debug("logout");
        }

        succeeded = false;
        commitSucceeded = false;
        return true;
    }
}
