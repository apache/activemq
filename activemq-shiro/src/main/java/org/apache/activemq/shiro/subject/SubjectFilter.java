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
package org.apache.activemq.shiro.subject;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.shiro.ConnectionReference;
import org.apache.activemq.shiro.DefaultSecurityContextFactory;
import org.apache.activemq.shiro.SecurityContextFactory;
import org.apache.activemq.shiro.env.EnvironmentFilter;
import org.apache.shiro.subject.Subject;

/**
 * The {@code SubjectFilter} ensures a Shiro {@link Subject} representing the client's identity is associated with
 * every connection to the ActiveMQ Broker.  The {@code Subject} is made available to downstream broker filters so
 * they may perform security checks as necessary.
 * <p/>
 * This implementation does not perform any security checks/assertions itself.  It is expected that other broker filters
 * will be configured after this one and those will perform any security behavior or checks as necessary.
 *
 * @since 5.10.0
 */
public class SubjectFilter extends EnvironmentFilter {

    private ConnectionSubjectFactory connectionSubjectFactory;
    private SecurityContextFactory securityContextFactory;

    public SubjectFilter() {
        this.connectionSubjectFactory = new DefaultConnectionSubjectFactory();
        this.securityContextFactory = new DefaultSecurityContextFactory();
    }

    public ConnectionSubjectFactory getConnectionSubjectFactory() {
        return connectionSubjectFactory;
    }

    public void setConnectionSubjectFactory(ConnectionSubjectFactory connectionSubjectFactory) {
        if (connectionSubjectFactory == null) {
            throw new IllegalArgumentException("ConnectionSubjectFactory argument cannot be null.");
        }
        this.connectionSubjectFactory = connectionSubjectFactory;
    }

    public SecurityContextFactory getSecurityContextFactory() {
        return this.securityContextFactory;
    }

    public void setSecurityContextFactory(SecurityContextFactory securityContextFactory) {
        if (securityContextFactory == null) {
            throw new IllegalArgumentException("SecurityContextFactory argument cannot be null.");
        }
        this.securityContextFactory = securityContextFactory;
    }

    protected Subject createSubject(ConnectionReference conn) {
        return this.connectionSubjectFactory.createSubject(conn);
    }

    protected SecurityContext createSecurityContext(SubjectConnectionReference conn) {
        return this.securityContextFactory.createSecurityContext(conn);
    }

    /**
     * Creates a {@link Subject} instance reflecting the specified Connection.  The {@code Subject} is then stored in
     * a {@link SecurityContext} instance which is set as the Connection's
     * {@link ConnectionContext#setSecurityContext(org.apache.activemq.security.SecurityContext) securityContext}.
     *
     * @param context state associated with the client's connection
     * @param info    info about the client's connection
     * @throws Exception if there is a problem creating a Subject or {@code SecurityContext} instance.
     */
    @Override
    public void addConnection(ConnectionContext context, ConnectionInfo info) throws Exception {

        if (isEnabled()) {

            SecurityContext secCtx = context.getSecurityContext();

            if (secCtx == null) {
                ConnectionReference conn = new ConnectionReference(context, info, getEnvironment());
                Subject subject = createSubject(conn);
                SubjectConnectionReference subjectConn = new SubjectConnectionReference(context, info, getEnvironment(), subject);
                secCtx = createSecurityContext(subjectConn);
                context.setSecurityContext(secCtx);
            }
        }

        try {
            super.addConnection(context, info);
        } catch (Exception e) {
            context.setSecurityContext(null);
            throw e;
        }
    }

    @Override
    public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
        try {
            super.removeConnection(context, info, error);
        } finally {
            context.setSecurityContext(null);
        }
    }
}
