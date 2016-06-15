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

import java.security.Principal;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.security.SecurityContext;
import org.apache.shiro.subject.Subject;

/**
 * ActiveMQ {@code SecurityContext} implementation that retains a Shiro {@code Subject} instance for use during
 * security checks and other security-related operations.
 *
 * @since 5.10.0
 */
public class SubjectSecurityContext extends SecurityContext {

    private final Subject subject;

    public SubjectSecurityContext(SubjectConnectionReference conn) {
        //The username might not be available at the time this object is instantiated (the Subject might be
        //anonymous).  Instead we override the getUserName() method below and that will always delegate to the
        //Subject to return the most accurate/freshest username available.
        super(null);
        this.subject = conn.getSubject();
    }

    public Subject getSubject() {
        return subject;
    }

    private static String getUsername(Subject subject) {
        if (subject != null) {
            Object principal = subject.getPrincipal();
            if (principal != null) {
                return String.valueOf(principal);
            }
        }
        return null;
    }

    @Override
    public String getUserName() {
        return getUsername(this.subject);
    }

    private static UnsupportedOperationException notAllowed(String methodName) {
        String msg = "Do not invoke the '" + methodName + "' method or use a broker filter that invokes it.  Use one " +
                "of the Shiro-based security filters instead.";
        return new UnsupportedOperationException(msg);
    }

    @Override
    public boolean isInOneOf(Set<?> allowedPrincipals) {
        throw notAllowed("isInOneOf");
    }

    @Override
    public ConcurrentMap<ActiveMQDestination, ActiveMQDestination> getAuthorizedWriteDests() {
        throw notAllowed("getAuthorizedWriteDests");
    }

    @Override
    public Set<Principal> getPrincipals() {
        throw notAllowed("getPrincipals");
    }
}
