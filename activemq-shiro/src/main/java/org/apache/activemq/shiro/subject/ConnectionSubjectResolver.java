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
import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.shiro.ConnectionReference;
import org.apache.shiro.subject.Subject;

/**
 * A {@code SubjectResolver} that acquires the current Subject from a {@link org.apache.activemq.shiro.ConnectionReference}.
 *
 * @since 5.10.0
 */
public class ConnectionSubjectResolver implements SubjectResolver {

    private final SubjectSecurityContext securityContext;

    public ConnectionSubjectResolver(ConnectionContext connCtx) {
        if (connCtx == null) {
            throw new IllegalArgumentException("ConnectionContext argument cannot be null.");
        }
        SecurityContext secCtx = connCtx.getSecurityContext();
        if (secCtx == null) {
            String msg = "There is no SecurityContext available on the ConnectionContext.  It " +
                    "is expected that a previous broker in the chain will create the SecurityContext prior to this " +
                    "resolver being invoked.  Ensure you have configured the SubjectPlugin and that it is " +
                    "configured before all other Shiro-dependent broker filters.";
            throw new IllegalArgumentException(msg);
        }
        if (!(secCtx instanceof SubjectSecurityContext)) {
            String msg = "The specified SecurityContext is expected to be a " + SubjectSecurityContext.class.getName() +
                    " instance.  The current instance's class: " + secCtx.getClass().getName();
            throw new IllegalArgumentException(msg);
        }
        this.securityContext = (SubjectSecurityContext) secCtx;
    }

    public ConnectionSubjectResolver(ConnectionReference conn) {
        this(conn.getConnectionContext());
    }

    @Override
    public Subject getSubject() {
        Subject subject = securityContext.getSubject();
        if (subject != null) {
            return subject;
        }
        String msg = "There is no Subject available in the SecurityContext.  Ensure " +
                "that the SubjectPlugin is configured before all other Shiro-dependent broker filters.";
        throw new IllegalStateException(msg);
    }
}
