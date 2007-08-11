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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * A LoginModule that allows for authentication based on SSL certificates.
 * 
 * Allows for subclasses to define methods used to verify user certificates and find user groups. 
 * Uses CertificateCallbacks to retrieve certificates.
 *  
 * @author sepandm@gmail.com (Sepand)
 *
 */
public abstract class CertificateLoginModule implements LoginModule {
    
    private CallbackHandler callbackHandler;
    private Subject subject;
    
    private X509Certificate certificates[];
    private String username = null;
    private Set groups = null;
    
    private Set principals = new HashSet();
    
    private static final Log log = LogFactory.getLog(CertificateLoginModule.class);
    private boolean debug;

    /**
     * Overriding to allow for proper initialization.
     * 
     * Standard JAAS.
     */
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map sharedState, Map options) {
        this.subject = subject;
        this.callbackHandler = callbackHandler;
        
        debug = "true".equalsIgnoreCase((String) options.get("debug"));
        
        if (debug) {
            log.debug("Initialized debug");
        }
    }

    /**
     * Overriding to allow for certificate-based login.
     * 
     * Standard JAAS.
     */
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
        certificates = ((CertificateCallback) callbacks[0]).getCertificates();
        
        username = getUserNameForCertificates(certificates);
        if ( username == null )
            throw new FailedLoginException("No user for client certificate: "
                + getDistinguishedName(certificates));

        groups = getUserGroups(username);
        
        if (debug) {
            log.debug("Certificate for user: " + username);
        }
        return true;
    }

    /**
     * Overriding to complete login process.
     * 
     * Standard JAAS.
     */
    public boolean commit() throws LoginException {
        principals.add(new UserPrincipal(username));

        String currentGroup = null;
        for (Iterator iter = groups.iterator(); iter.hasNext(); ) {
            currentGroup = (String)iter.next();
            principals.add(new GroupPrincipal(currentGroup));
        }

        subject.getPrincipals().addAll(principals);

        clear();

        if (debug) {
            log.debug("commit");
        }
        return true;
    }

    /**
     * Standard JAAS override.
     */
    public boolean abort() throws LoginException {
        clear();

        if (debug) {
            log.debug("abort");
        }
        return true;
    }

    /**
     * Standard JAAS override.
     */
    public boolean logout() {
        subject.getPrincipals().removeAll(principals);
        principals.clear();

        if (debug) {
            log.debug("logout");
        }
        return true;
    }
    
    /**
     * Helper method.
     */
    private void clear() {
        groups.clear();
        certificates = null;
    }
    
    /**
     * Should return a unique name corresponding to the certificates given.
     * 
     * The name returned will be used to look up access levels as well as
     *      group associations.
     *      
     * @param dn The distinguished name.
     * @return The unique name if the certificate is recognized, null otherwise.
     */
    protected abstract String getUserNameForCertificates(final X509Certificate[] certs) throws LoginException;
    
    /**
     * Should return a set of the groups this user belongs to.
     * 
     * The groups returned will be added to the user's credentials.
     * 
     * @param username The username of the client. This is the same name that
     *      getUserNameForDn returned for the user's DN.
     * @return A Set of the names of the groups this user belongs to.
     */
    protected abstract Set getUserGroups(final String username) throws LoginException;

    protected String getDistinguishedName(final X509Certificate[] certs) {
        if (certs != null && certs.length > 0 && certs[0] != null) {
            return certs[0].getSubjectDN().getName();
        } else {
            return null;
        }
    }

}
