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
package org.apache.activemq.shiro.authc;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.shiro.ConnectionReference;
import org.apache.activemq.shiro.env.EnvironmentFilter;
import org.apache.activemq.shiro.subject.ConnectionSubjectResolver;
import org.apache.activemq.shiro.subject.SubjectConnectionReference;
import org.apache.activemq.shiro.subject.SubjectSecurityContext;
import org.apache.shiro.authc.AuthenticationException;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.subject.Subject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The {@code AuthenticationFilter} enforces if authentication is required before allowing the broker filter chain
 * to continue.
 * <p/>
 * This implementation performs a connection-level authentication assertion:  If the {@link Subject} associated with the
 * connection<b>*</b> is not authenticated, and the
 * {@link AuthenticationPolicy AuthenticationPolicy} requires the {@code Subject} to be authenticated, it will attempt
 * to {@link Subject#login(org.apache.shiro.authc.AuthenticationToken) login} the Subject automatically.  The
 * {@link AuthenticationToken} used to login is created by the
 * {@link #getAuthenticationTokenFactory() authenticationTokenFactory}, typically by acquiring any credentials
 * associated with the connection.
 * <p/>
 * Once the connection's {@code Subject} is authenticated as necessary, the broker filter chain will continue
 * as expected.
 * <p/>
 * <b>*</b>: The upstream {@link org.apache.activemq.shiro.subject.SubjectFilter} is expected to execute before this one, ensuring a Subject instance
 * is already associated with the connection.
 *
 * @since 5.10.0
 */
public class AuthenticationFilter extends EnvironmentFilter {

    private static final Logger LOG = LoggerFactory.getLogger(AuthenticationFilter.class);

    private AuthenticationPolicy authenticationPolicy;
    private AuthenticationTokenFactory authenticationTokenFactory;

    public AuthenticationFilter() {
        this.authenticationPolicy = new DefaultAuthenticationPolicy();
        this.authenticationTokenFactory = new DefaultAuthenticationTokenFactory();
    }

    public AuthenticationPolicy getAuthenticationPolicy() {
        return authenticationPolicy;
    }

    public void setAuthenticationPolicy(AuthenticationPolicy authenticationPolicy) {
        this.authenticationPolicy = authenticationPolicy;
    }

    public AuthenticationTokenFactory getAuthenticationTokenFactory() {
        return authenticationTokenFactory;
    }

    public void setAuthenticationTokenFactory(AuthenticationTokenFactory authenticationTokenFactory) {
        this.authenticationTokenFactory = authenticationTokenFactory;
    }

    protected Subject getSubject(ConnectionReference conn) {
        return new ConnectionSubjectResolver(conn).getSubject();
    }

    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {

        if (isEnabled()) { //disabled means don't enforce authentication (i.e. allow anonymous access):

            Subject subject = getSubject(new ConnectionReference(context, info, getEnvironment()));

            if (!subject.isAuthenticated()) {

                SubjectConnectionReference connection = new SubjectConnectionReference(context, info, getEnvironment(), subject);

                if (this.authenticationPolicy.isAuthenticationRequired(connection)) {
                    AuthenticationToken token = this.authenticationTokenFactory.getAuthenticationToken(connection);
                    if (token == null) {
                        String msg = "Unable to obtain authentication credentials for newly established connection.  " +
                                "Authentication is required.";
                        throw new AuthenticationException(msg);
                    }
                    //token is not null - login the current subject:
                    subject.login(token);
                }
            }
        }

        super.addConnection(context, info);
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        try {
            super.removeConnection(context, info, error);
        } finally {
            SecurityContext secCtx = context.getSecurityContext();

            if (secCtx instanceof SubjectSecurityContext) {

                SubjectSecurityContext subjectSecurityContext = (SubjectSecurityContext) secCtx;
                Subject subject = subjectSecurityContext.getSubject();

                if (subject != null) {
                    try {
                        subject.logout();
                    } catch (Throwable t) {
                        String msg = "Unable to cleanly logout connection Subject during connection removal.  This is " +
                                "unexpected but not critical: it can be safely ignored because the " +
                                "connection will no longer be used.";
                        LOG.info(msg, t);
                    }
                }
            }
        }
    }
}
