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

import org.apache.activemq.shiro.subject.SubjectConnectionReference;
import org.apache.shiro.authc.AuthenticationToken;

/**
 * A {@code AuthenticationTokenFactory} inspects a newly-added ActiveMQ connection and returns a Shiro
 * {@link AuthenticationToken} instance representing credentials associated with the connection.  These credentials can
 * be used to {@link org.apache.shiro.subject.Subject#login(org.apache.shiro.authc.AuthenticationToken) authenticate}
 * the connection, allowing for later identity and authorization (access control) checks.
 *
 * @see AuthenticationFilter#addConnection(org.apache.activemq.broker.ConnectionContext, org.apache.activemq.command.ConnectionInfo)
 * @since 5.10.0
 */
public interface AuthenticationTokenFactory {

    /**
     * Returns a Shiro {@code AuthenticationToken} instance that should be used to authenticate the connection's
     * {@link org.apache.shiro.subject.Subject}, or {@code null} if no authentication information can be obtained.
     * <p/>
     * If no {@code AuthenticationToken} can be obtained, the connection's Subject will be considered anonymous and any
     * downstream security checks that enforce authentication or authorization will fail (as would be expected).
     *
     * @param ref the subject's connection
     * @return a Shiro {@code AuthenticationToken} instance that should be used to authenticate the connection's
     *         {@link org.apache.shiro.subject.Subject}, or {@code null} if no authentication information can be obtained.
     * @throws Exception if there is a problem acquiring/creating an expected {@code AuthenticationToken}.
     */
    AuthenticationToken getAuthenticationToken(SubjectConnectionReference ref) throws Exception;
}
