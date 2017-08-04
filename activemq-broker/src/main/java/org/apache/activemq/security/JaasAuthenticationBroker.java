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
            // Set the TCCL since it seems JAAS needs it to find the login module classes.
            ClassLoader original = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(JaasAuthenticationBroker.class.getClassLoader());
            SecurityContext securityContext = null;
            try {
                securityContext = authenticate(info.getUserName(), info.getPassword(), null);
                context.setSecurityContext(securityContext);
                securityContexts.add(securityContext);
                super.addConnection(context, info);
            } catch (Exception error) {
                if (securityContext != null) {
                    securityContexts.remove(securityContext);
                }
                context.setSecurityContext(null);
                throw error;
            } finally {
                Thread.currentThread().setContextClassLoader(original);
            }
        } else {
            super.addConnection(context, info);
        }
    }

    @Override
    public SecurityContext authenticate(String username, String password, X509Certificate[] certificates) throws SecurityException {
        SecurityContext result = null;
        JassCredentialCallbackHandler callback = new JassCredentialCallbackHandler(username, password);
        try {
            LoginContext lc = new LoginContext(jassConfiguration, callback);
            lc.login();
            Subject subject = lc.getSubject();

            result = new JaasSecurityContext(username, subject);
        } catch (Exception ex) {
            throw new SecurityException("User name [" + username + "] or password is invalid.", ex);
        }

        return result;
    }
}
