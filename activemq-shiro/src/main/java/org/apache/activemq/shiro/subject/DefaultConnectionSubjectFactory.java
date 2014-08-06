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
import org.apache.activemq.shiro.authc.DefaultAuthenticationPolicy;
import org.apache.shiro.subject.Subject;

/**
 * @since 5.10.0
 */
public class DefaultConnectionSubjectFactory implements ConnectionSubjectFactory {

    private AuthenticationPolicy authenticationPolicy;

    public DefaultConnectionSubjectFactory() {
        this.authenticationPolicy = new DefaultAuthenticationPolicy();
    }

    public AuthenticationPolicy getAuthenticationPolicy() {
        return authenticationPolicy;
    }

    public void setAuthenticationPolicy(AuthenticationPolicy authenticationPolicy) {
        this.authenticationPolicy = authenticationPolicy;
    }

    @Override
    public Subject createSubject(ConnectionReference conn) {

        Subject.Builder builder = new Subject.Builder(conn.getEnvironment().getSecurityManager());

        authenticationPolicy.customizeSubject(builder, conn);

        return builder.buildSubject();
    }
}
