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

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.login.LoginContext;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerFilter;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.jaas.JaasCertificateCallbackHandler;
import org.apache.activemq.jaas.UserPrincipal;

/**
 * A JAAS Authentication Broker that uses SSL Certificates. This class will
 * provide the JAAS framework with a JaasCertificateCallbackHandler that will
 * grant JAAS access to incoming connections' SSL certificate chains. NOTE:
 * There is a chance that the incoming connection does not have a valid
 * certificate (has null).
 * 
 * @author sepandm@gmail.com (Sepand)
 */
public class JaasCertificateAuthenticationBroker extends BrokerFilter {
    private final String jaasConfiguration;

    /**
     * Simple constructor. Leaves everything to superclass.
     * 
     * @param next The Broker that does the actual work for this Filter.
     * @param jassConfiguration The JAAS domain configuration name (refere to
     *                JAAS documentation).
     */
    public JaasCertificateAuthenticationBroker(Broker next, String jaasConfiguration) {
        super(next);

        this.jaasConfiguration = jaasConfiguration;
    }

    /**
     * Overridden to allow for authentication based on client certificates.
     * Connections being added will be authenticated based on their certificate
     * chain and the JAAS module specified through the JAAS framework. NOTE: The
     * security context's username will be set to the first UserPrincipal
     * created by the login module.
     * 
     * @param context The context for the incoming Connection.
     * @param info The ConnectionInfo Command representing the incoming
     *                connection.
     */
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {

        if (context.getSecurityContext() == null) {
            if (!(info.getTransportContext() instanceof X509Certificate[])) {
                throw new SecurityException("Unable to authenticate transport without SSL certificate.");
            }

            // Set the TCCL since it seems JAAS needs it to find the login
            // module classes.
            ClassLoader original = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(JaasAuthenticationBroker.class.getClassLoader());
            try {
                // Do the login.
                try {
                    CallbackHandler callback = new JaasCertificateCallbackHandler((X509Certificate[])info.getTransportContext());
                    LoginContext lc = new LoginContext(jaasConfiguration, callback);
                    lc.login();
                    Subject subject = lc.getSubject();

                    String dnName = "";

                    for (Principal principal : subject.getPrincipals()) {
                        if (principal instanceof UserPrincipal) {
                            dnName = ((UserPrincipal)principal).getName();
                            break;
                        }
                    }
                    SecurityContext s = new JaasCertificateSecurityContext(dnName, subject, (X509Certificate[])info.getTransportContext());
                    context.setSecurityContext(s);
                } catch (Exception e) {
                    throw new SecurityException("User name [" + info.getUserName() + "] or password is invalid. " + e.getMessage(), e);
                }
            } finally {
                Thread.currentThread().setContextClassLoader(original);
            }
        }
        super.addConnection(context, info);
    }

    /**
     * Overriding removeConnection to make sure the security context is cleaned.
     */
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        super.removeConnection(context, info, error);

        context.setSecurityContext(null);
    }
}
