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

import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.shiro.subject.SubjectConnectionReference;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.authc.UsernamePasswordToken;

/**
 * Default implementation of the {@link AuthenticationTokenFactory} interface that returns
 * {@link org.apache.shiro.authc.UsernamePasswordToken UsernamePasswordToken} instances based on inspecting the
 * {@link ConnectionInfo}.
 *
 * @since 5.10.0
 */
public class DefaultAuthenticationTokenFactory implements AuthenticationTokenFactory {

    public DefaultAuthenticationTokenFactory() {
    }

    /**
     * Returns a new {@link UsernamePasswordToken} instance populated based on the ConnectionInfo's
     * {@link org.apache.activemq.command.ConnectionInfo#getUserName() userName} and
     * {@link org.apache.activemq.command.ConnectionInfo#getPassword() password} properties.
     *
     * @param conn the subject's connection
     * @return a new {@link UsernamePasswordToken} instance populated based on the ConnectionInfo's
     *         ConnectionInfo's {@link org.apache.activemq.command.ConnectionInfo#getUserName() userName} and
     *         {@link org.apache.activemq.command.ConnectionInfo#getPassword() password} properties.
     */
    @Override
    public AuthenticationToken getAuthenticationToken(SubjectConnectionReference conn) {

        String username = conn.getConnectionInfo().getUserName();
        String password = conn.getConnectionInfo().getPassword();

        if (username == null && password == null) {
            //no identity or credentials provided by the client for the connection - return null to reflect this
            return null;
        }

        return new UsernamePasswordToken(username, password);
    }
}
