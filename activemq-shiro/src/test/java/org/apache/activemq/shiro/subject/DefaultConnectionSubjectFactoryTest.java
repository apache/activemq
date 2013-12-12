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

import org.apache.activemq.shiro.ConnectionReference;
import org.apache.activemq.shiro.authc.AuthenticationPolicy;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @since 5.10.0
 */
public class DefaultConnectionSubjectFactoryTest {

    private DefaultConnectionSubjectFactory factory;

    @Before
    public void setUp() {
        this.factory = new DefaultConnectionSubjectFactory();
    }

    @Test
    public void testSetAuthenticationPolicy() {
        AuthenticationPolicy policy = new AuthenticationPolicy() {
            @Override
            public void customizeSubject(Subject.Builder subjectBuilder, ConnectionReference ref) {
                //To change body of implemented methods use File | Settings | File Templates.
            }

            @Override
            public boolean isAuthenticationRequired(SubjectConnectionReference ref) {
                return false;  //To change body of implemented methods use File | Settings | File Templates.
            }
        };
        factory.setAuthenticationPolicy(policy);
        assertSame(policy, factory.getAuthenticationPolicy());
    }
}
