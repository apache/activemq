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
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.jaas.JassCredentialCallbackHandler;

/**
 * Logs a user in using JAAS.
 * 
 * 
 */
public class JaasAuthenticationBroker extends BrokerFilter {

    private final String jassConfiguration;
    private final CopyOnWriteArrayList<SecurityContext> securityContexts = new CopyOnWriteArrayList<SecurityContext>();

    public JaasAuthenticationBroker(Broker next, String jassConfiguration) {
        super(next);
        this.jassConfiguration = jassConfiguration;
    }

    static class JaasSecurityContext extends SecurityContext {

        private final Subject subject;

        public JaasSecurityContext(String userName, Subject subject) {
            super(userName);
            this.subject = subject;
        }

        public Set<Principal> getPrincipals() {
            return subject.getPrincipals();
        }

    }

    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {

        if (context.getSecurityContext() == null) {
            // Set the TCCL since it seems JAAS needs it to find the login
            // module classes.
            ClassLoader original = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(JaasAuthenticationBroker.class.getClassLoader());
            try {
                // Do the login.
                try {
                    JassCredentialCallbackHandler callback = new JassCredentialCallbackHandler(info
                        .getUserName(), info.getPassword());
                    LoginContext lc = new LoginContext(jassConfiguration, callback);
                    lc.login();
                    Subject subject = lc.getSubject();

                    SecurityContext s = new JaasSecurityContext(info.getUserName(), subject);
                    context.setSecurityContext(s);
                    securityContexts.add(s);
                } catch (Exception e) {
                    throw (SecurityException)new SecurityException("User name or password is invalid.")
                        .initCause(e);
                }
            } finally {
                Thread.currentThread().setContextClassLoader(original);
            }
        }
        super.addConnection(context, info);
    }

    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error)
        throws Exception {
        super.removeConnection(context, info, error);
        if (securityContexts.remove(context.getSecurityContext())) {
            context.setSecurityContext(null);
        }
    }

    /**
     * Previously logged in users may no longer have the same access anymore.
     * Refresh all the logged into users.
     */
    public void refresh() {
        for (Iterator<SecurityContext> iter = securityContexts.iterator(); iter.hasNext();) {
            SecurityContext sc = iter.next();
            sc.getAuthorizedReadDests().clear();
            sc.getAuthorizedWriteDests().clear();
        }
    }
}
