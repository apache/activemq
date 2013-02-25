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
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.login.LoginContext;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.jaas.JassCredentialCallbackHandler;

/**
 * Logs a user in using JAAS.
 *
 *
 */
public class JaasAuthenticationBroker extends AbstractAuthenticationBroker {

    private final String jassConfiguration;

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

        @Override
        public Set<Principal> getPrincipals() {
            return subject.getPrincipals();
        }
    }

    @Override
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
                    throw (SecurityException)new SecurityException("User name [" + info.getUserName() + "] or password is invalid.")
                        .initCause(e);
                }
            } finally {
                Thread.currentThread().setContextClassLoader(original);
            }
        }
        super.addConnection(context, info);
    }
}
