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

import org.apache.activemq.shiro.ConnectionReference;
import org.apache.activemq.shiro.subject.SubjectConnectionReference;
import org.apache.shiro.subject.Subject;

/**
 * An {@code AuthenticationPolicy} customizes the behavior of the {@link AuthenticationFilter}, such as whether or not
 * authentication is required or how to represent trusted/known {@code Subject} identities.
 * <p/>
 * Most will find customizing properties on the {@link DefaultAuthenticationPolicy} easier than implementing this
 * interface directly.
 *
 * @see DefaultAuthenticationPolicy
 * @since 5.10.0
 */
public interface AuthenticationPolicy {

    /**
     * Allows customization of the {@code Subject} being built for the specified client
     * connection.  This allows for any pre-existing connection-specific identity or state to be applied to the
     * {@link Subject.Builder} before the {@code Subject} instance is actually created.
     * <p/>
     * <b>NOTE:</b> This method is called by the {@link org.apache.activemq.shiro.subject.SubjectFilter SubjectFilter} <em>before</em> the filter chain
     * is executed (and before an authentication attempt occurs).  Implementations <b><em>MUST NOT</em></b>
     * attempt to actually {@link org.apache.shiro.subject.Subject.Builder#buildSubject() build} the subject or perform
     * an authentication attempt in this method.
     *
     * @param subjectBuilder the builder for the Subject that will be created representing the associated client connection
     * @param ref            a reference to the client's connection metadata
     * @see org.apache.activemq.shiro.subject.SubjectFilter
     */
    void customizeSubject(Subject.Builder subjectBuilder, ConnectionReference ref);

    /**
     * Returns {@code true} if the connection's {@code Subject} instance should be authenticated, {@code false} otherwise.
     *
     * @param ref the subject's connection
     * @return {@code true} if the connection's {@code Subject} instance should be authenticated, {@code false} otherwise.
     */
    boolean isAuthenticationRequired(SubjectConnectionReference ref);
}
