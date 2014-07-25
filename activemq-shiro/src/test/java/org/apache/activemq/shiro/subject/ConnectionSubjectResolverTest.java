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
import org.apache.shiro.env.DefaultEnvironment;
import org.apache.shiro.subject.Subject;
import org.junit.Test;

import java.security.Principal;
import java.util.Set;

/**
 * @since 5.10.0
 */
public class ConnectionSubjectResolverTest {

    @Test(expected = IllegalArgumentException.class)
    public void testNullConstructorArg() {
        new ConnectionSubjectResolver((ConnectionContext)null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNullSecurityContext() {
        SubjectConnectionReference reference =
                new SubjectConnectionReference(new ConnectionContext(), new ConnectionInfo(),
                        new DefaultEnvironment(), new SubjectAdapter());

        new ConnectionSubjectResolver(reference);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testNonSubjectSecurityContext() {
        SubjectConnectionReference reference =
                new SubjectConnectionReference(new ConnectionContext(), new ConnectionInfo(),
                        new DefaultEnvironment(), new SubjectAdapter());
        reference.getConnectionContext().setSecurityContext(new SecurityContext("") {
            @Override
            public Set<Principal> getPrincipals() {
                return null;
            }
        });

        new ConnectionSubjectResolver(reference);
    }

    @Test(expected = IllegalStateException.class)
    public void testNullSubject() {

        SubjectConnectionReference reference =
                new SubjectConnectionReference(new ConnectionContext(), new ConnectionInfo(),
                        new DefaultEnvironment(), new SubjectAdapter());
        reference.getConnectionContext().setSecurityContext(new SubjectSecurityContext(reference) {
            @Override
            public Subject getSubject() {
                return null;
            }
        });

        ConnectionSubjectResolver resolver = new ConnectionSubjectResolver(reference);
        resolver.getSubject();
    }

}
