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
package org.apache.activemq.shiro;

import org.apache.activemq.security.SecurityContext;
import org.apache.activemq.shiro.subject.SubjectConnectionReference;
import org.apache.activemq.shiro.subject.SubjectSecurityContext;
import org.apache.shiro.env.Environment;

/**
 * Default {@code SecurityContextFactory} implementation that creates
 * {@link org.apache.activemq.shiro.subject.SubjectSecurityContext} instances, allowing the connection's {@code Subject} and the Shiro
 * {@link Environment} to be available to downstream security broker filters.
 *
 * @since 5.10.0
 */
public class DefaultSecurityContextFactory implements SecurityContextFactory {

    /**
     * Returns a new {@link org.apache.activemq.shiro.subject.SubjectSecurityContext} instance, allowing the connection's {@code Subject} and the Shiro
     * {@link Environment} to be available to downstream security broker filters.
     *
     * @param conn the subject's connection
     * @return a new {@link org.apache.activemq.shiro.subject.SubjectSecurityContext} instance, allowing the connection's {@code Subject} and the Shiro
     *         {@link Environment} to be available to downstream security broker filters.
     */
    @Override
    public SecurityContext createSecurityContext(SubjectConnectionReference conn) {
        return new SubjectSecurityContext(conn);
    }
}
