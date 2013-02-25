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
    private final Map<String,String> userPasswords;
    private final Map<String,Set<Principal>> userGroups;

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

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {

        SecurityContext s = context.getSecurityContext();
        if (s == null) {
            // Check the username and password.
            if (anonymousAccessAllowed && info.getUserName() == null && info.getPassword() == null) {
                info.setUserName(anonymousUser);
                s = new SecurityContext(info.getUserName()) {
                    @Override
                    public Set<Principal> getPrincipals() {
                        Set<Principal> groups = new HashSet<Principal>();
                        groups.add(new GroupPrincipal(anonymousGroup));
                        return groups;
                    }
                };
            } else {
                String pw = userPasswords.get(info.getUserName());
                if (pw == null || !pw.equals(info.getPassword())) {
                    throw new SecurityException(
                            "User name [" + info.getUserName() + "] or password is invalid.");
                }

                final Set<Principal> groups = userGroups.get(info.getUserName());
                s = new SecurityContext(info.getUserName()) {
                    @Override
                    public Set<Principal> getPrincipals() {
                        return groups;
                    }
                };
            }

            context.setSecurityContext(s);
            securityContexts.add(s);
        }

        try {
            super.addConnection(context, info);
        } catch (Exception e) {
            securityContexts.remove(s);
            context.setSecurityContext(null);
            throw e;
        }
    }
}
