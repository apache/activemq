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
package org.apache.activemq.security;

import java.security.Principal;
import java.security.cert.X509Certificate;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.jaas.GroupPrincipal;

/**
 * Handles authenticating a users against a simple user name/password map.
 */
public class SimpleAuthenticationBroker extends AbstractAuthenticationBroker {

    private boolean anonymousAccessAllowed = false;
    private String anonymousUser;
    private String anonymousGroup;
    private Map<String,String> userPasswords;
    private Map<String,Set<Principal>> userGroups;

    public SimpleAuthenticationBroker(Broker next, Map<String,String> userPasswords, Map<String,Set<Principal>> userGroups) {
        super(next);
        this.userPasswords = userPasswords;
        this.userGroups = userGroups;
    }

    public void setAnonymousAccessAllowed(boolean anonymousAccessAllowed) {
        this.anonymousAccessAllowed = anonymousAccessAllowed;
    }

    public void setAnonymousUser(String anonymousUser) {
        this.anonymousUser = anonymousUser;
    }

    public void setAnonymousGroup(String anonymousGroup) {
        this.anonymousGroup = anonymousGroup;
    }

    public void setUserPasswords(Map<String,String> value) {
        userPasswords = value;
    }

    public void setUserGroups(Map<String, Set<Principal>> value) {
        userGroups = value;
    }

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {
        SecurityContext securityContext = context.getSecurityContext();
        if (securityContext == null) {
            securityContext = authenticate(info.getUserName(), info.getPassword(), null);
            context.setSecurityContext(securityContext);
            securityContexts.add(securityContext);
        }

        try {
            super.addConnection(context, info);
        } catch (Exception e) {
            securityContexts.remove(securityContext);
            context.setSecurityContext(null);
            throw e;
        }
    }

    @Override
    public SecurityContext authenticate(String username, String password, X509Certificate[] certificates) throws SecurityException {
        SecurityContext securityContext = null;

        // Check the username and password.
        if (anonymousAccessAllowed && username == null && password == null) {
            username = anonymousUser;
            securityContext = new SecurityContext(username) {
                @Override
                public Set<Principal> getPrincipals() {
                    Set<Principal> groups = new HashSet<Principal>();
                    groups.add(new GroupPrincipal(anonymousGroup));
                    return groups;
                }
            };
        } else {
            String pw = userPasswords.get(username);
            if (pw == null || !pw.equals(password)) {
                throw new SecurityException("User name [" + username + "] or password is invalid.");
            }

            final Set<Principal> groups = userGroups.get(username);
            securityContext = new SecurityContext(username) {
                @Override
                public Set<Principal> getPrincipals() {
                    return groups;
                }
            };
        }

        return securityContext;
    }
}
