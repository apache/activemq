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
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A LoginModule that allows for authentication based on SSL certificates.
 * Allows for subclasses to define methods used to verify user certificates and
 * find user groups. Uses CertificateCallbacks to retrieve certificates.
 * 
 * @author sepandm@gmail.com (Sepand)
 */
public abstract class CertificateLoginModule extends PropertiesLoader implements LoginModule {

    private static final Logger LOG = Logger.getLogger(CertificateLoginModule.class.getName());

    private CallbackHandler callbackHandler;
    private Subject subject;

    private String username;
    private Set<Principal> principals = new HashSet<Principal>();

    /** the authentication status*/
    private boolean succeeded = false;
    private boolean commitSucceeded = false;

    /**
     * Overriding to allow for proper initialization. Standard JAAS.
     */
    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        init(options);
    }

    /**
     * Overriding to allow for certificate-based login. Standard JAAS.
     */
    @Override
    public boolean login() throws LoginException {
        Callback[] callbacks = new Callback[1];

        callbacks[0] = new CertificateCallback();
        try {
            callbackHandler.handle(callbacks);
        } catch (IOException ioe) {
            throw new LoginException(ioe.getMessage());
        } catch (UnsupportedCallbackException uce) {
            throw new LoginException(uce.getMessage() + " Unable to obtain client certificates.");
        }
        X509Certificate[] certificates = ((CertificateCallback)callbacks[0]).getCertificates();

        username = getUserNameForCertificates(certificates);
        if (username == null) {
            throw new FailedLoginException("No user for client certificate: " + getDistinguishedName(certificates));
        }

        if (debug) {
            LOG.log(Level.FINE, "Certificate for user: " + username);
        }
        succeeded = true;
        return true;
    }

    /**
     * Overriding to complete login process. Standard JAAS.
     */
    @Override
    public boolean commit() throws LoginException {
        if (debug) {
            LOG.log(Level.FINE, "commit");
        }

        if (!succeeded) {
            clear();
            return false;
        }

        principals.add(new UserPrincipal(username));

        for (String group : getUserGroups(username)) {
             principals.add(new GroupPrincipal(group));
        }

        subject.getPrincipals().addAll(principals);

        username = null;
        commitSucceeded = true;
        return true;
    }

    /**
     * Standard JAAS override.
     */
    @Override
    public boolean abort() throws LoginException {
        if (debug) {
            LOG.log(Level.FINE, "abort");
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

    /**
     * Standard JAAS override.
     */
    @Override
    public boolean logout() {
        subject.getPrincipals().removeAll(principals);
        clear();

        if (debug) {
            LOG.log(Level.FINE, "logout");
        }

        succeeded = false;
        commitSucceeded = false;
        return true;
    }

    /**
     * Helper method.
     */
    private void clear() {
        username = null;
        principals.clear();
    }

    /**
     * Should return a unique name corresponding to the certificates given. The
     * name returned will be used to look up access levels as well as group
     * associations.
     * 
     * @param certs The distinguished name.
     * @return The unique name if the certificate is recognized, null otherwise.
     */
    protected abstract String getUserNameForCertificates(final X509Certificate[] certs) throws LoginException;

    /**
     * Should return a set of the groups this user belongs to. The groups
     * returned will be added to the user's credentials.
     * 
     * @param username The username of the client. This is the same name that
     *                getUserNameForDn returned for the user's DN.
     * @return A Set of the names of the groups this user belongs to.
     */
    protected abstract Set<String> getUserGroups(final String username) throws LoginException;

    protected String getDistinguishedName(final X509Certificate[] certs) {
        if (certs != null && certs.length > 0 && certs[0] != null) {
            return certs[0].getSubjectDN().getName();
        } else {
            return null;
        }
    }

}
