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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPluginSupport;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.shiro.subject.SubjectAdapter;
import org.apache.activemq.shiro.subject.SubjectConnectionReference;
import org.apache.activemq.shiro.subject.SubjectSecurityContext;
import org.apache.shiro.authc.AuthenticationToken;
import org.apache.shiro.env.DefaultEnvironment;
import org.apache.shiro.subject.Subject;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * @since 5.10.0
 */
public class AuthenticationFilterTest {

    AuthenticationFilter filter = new AuthenticationFilter();

    @Test
    public void testSetAuthenticationTokenFactory() {
        AuthenticationTokenFactory factory = new AuthenticationTokenFactory() {
            @Override
            public AuthenticationToken getAuthenticationToken(SubjectConnectionReference ref) throws Exception {
                return null;
            }
        };
        filter.setAuthenticationTokenFactory(factory);
        assertSame(factory, filter.getAuthenticationTokenFactory());
    }

    @Test
    public void testRemoveAuthenticationWithLogoutThrowable() throws Exception {

        final boolean[] invoked = new boolean[1];

        Broker broker = new BrokerPluginSupport() {
            @Override
            public void removeConnection(ConnectionContext context, ConnectionInfo info, Throwable error) throws Exception {
                invoked[0] = true;
            }
        };

        DefaultEnvironment env = new DefaultEnvironment();

        filter.setNext(broker);
        filter.setEnvironment(env);

        Subject subject = new SubjectAdapter() {
            @Override
            public void logout() {
                throw new RuntimeException("Simulated failure.");
            }
        };

        ConnectionContext ctx = new ConnectionContext();
        ConnectionInfo info = new ConnectionInfo();
        SubjectConnectionReference conn = new SubjectConnectionReference(ctx, info, env, subject);
        SubjectSecurityContext ssc = new SubjectSecurityContext(conn);
        ctx.setSecurityContext(ssc);

        filter.removeConnection(ctx, info, null);

        assertTrue(invoked[0]);
    }
}
