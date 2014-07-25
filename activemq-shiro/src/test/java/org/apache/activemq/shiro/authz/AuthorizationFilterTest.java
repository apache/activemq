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
package org.apache.activemq.shiro.authz;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.ProducerBrokerExchange;
import org.apache.activemq.broker.StubBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQTextMessage;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.ConnectionInfo;
import org.apache.activemq.command.ConsumerInfo;
import org.apache.activemq.command.DestinationInfo;
import org.apache.activemq.command.ProducerInfo;
import org.apache.activemq.shiro.subject.SubjectAdapter;
import org.apache.activemq.shiro.subject.SubjectConnectionReference;
import org.apache.activemq.shiro.subject.SubjectSecurityContext;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.UnauthorizedException;
import org.apache.shiro.env.Environment;
import org.apache.shiro.subject.PrincipalCollection;
import org.apache.shiro.subject.SimplePrincipalCollection;
import org.apache.shiro.subject.Subject;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;

/**
 * @since 5.10.0
 */
public class AuthorizationFilterTest {

    private AuthorizationFilter filter;
    private StubBroker nextBroker;

    @Before
    public void setUp() {
        filter = new AuthorizationFilter();
        nextBroker = new StubBroker();
        filter.setNext(nextBroker);
    }

    @Test
    public void testDefaults() {
        ActionPermissionResolver resolver = filter.getActionPermissionResolver();
        assertNotNull(resolver);
        assertTrue(resolver instanceof DestinationActionPermissionResolver);
    }

    @Test
    public void testSetActionPermissionResolver() {
        ActionPermissionResolver resolver = new DestinationActionPermissionResolver();
        filter.setActionPermissionResolver(resolver);
        assertSame(resolver, filter.getActionPermissionResolver());
    }

    private ConnectionContext createContext(Subject subject) {
        ConnectionContext ctx = new ConnectionContext();
        ConnectionInfo info = new ConnectionInfo();
        Environment environment = new Environment() {
            @Override
            public org.apache.shiro.mgt.SecurityManager getSecurityManager() {
                return null; //not needed in this test.
            }
        };
        SubjectConnectionReference ref = new SubjectConnectionReference(ctx, info, environment, subject);
        SubjectSecurityContext secCtx = new SubjectSecurityContext(ref);
        ctx.setSecurityContext(secCtx);
        return ctx;
    }

    @Test
    public void testSubjectToString() {
        Subject subject = new PermsSubject() {
            @Override
            public PrincipalCollection getPrincipals() {
                return null;
            }
        };
        String string = filter.toString(subject);
        assertEquals("", string);
    }

    @Test(expected=UnauthorizedException.class)
    public void testAddDestinationInfoNotAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);
        DestinationInfo info = new DestinationInfo(null, DestinationInfo.ADD_OPERATION_TYPE, dest);

        Subject subject = new PermsSubject();
        ConnectionContext context = createContext(subject);

        filter.addDestinationInfo(context, info);
    }

    @Test
    public void testAddDestinationInfoAuthorized() throws Exception {

        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);
        DestinationInfo info = new DestinationInfo(null, DestinationInfo.ADD_OPERATION_TYPE, dest);

        Subject subject = new PermsSubject() {
            @Override
            public boolean isPermitted(Permission toCheck) {
                Permission assigned = createPerm("topic:myTopic:create");
                assertEquals(assigned.toString(), toCheck.toString());
                return assigned.implies(toCheck);
            }
        };

        ConnectionContext context = createContext(subject);

        filter.addDestinationInfo(context, info);
    }

    @Test(expected=UnauthorizedException.class)
    public void testAddDestinationNotAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);

        Subject subject = new PermsSubject();
        ConnectionContext context = createContext(subject);

        filter.addDestination(context, dest, true);
    }

    @Test
    public void testAddDestinationAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);

        Subject subject = new PermsSubject() {
            @Override
            public boolean isPermitted(Permission toCheck) {
                Permission assigned = createPerm("topic:myTopic:create");
                assertEquals(assigned.toString(), toCheck.toString());
                return assigned.implies(toCheck);
            }
        };

        ConnectionContext context = createContext(subject);

        filter.addDestination(context, dest, true);
    }

    @Test(expected=UnauthorizedException.class)
    public void testRemoveDestinationInfoNotAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);
        DestinationInfo info = new DestinationInfo(null, DestinationInfo.REMOVE_OPERATION_TYPE, dest);

        Subject subject = new PermsSubject();
        ConnectionContext context = createContext(subject);

        filter.removeDestinationInfo(context, info);
    }

    @Test
    public void testRemoveDestinationInfoAuthorized() throws Exception {

        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);
        DestinationInfo info = new DestinationInfo(null, DestinationInfo.REMOVE_OPERATION_TYPE, dest);

        Subject subject = new PermsSubject() {
            @Override
            public boolean isPermitted(Permission toCheck) {
                Permission assigned = createPerm("topic:myTopic:remove");
                assertEquals(assigned.toString(), toCheck.toString());
                return assigned.implies(toCheck);
            }
        };

        ConnectionContext context = createContext(subject);

        filter.removeDestinationInfo(context, info);
    }

    @Test(expected=UnauthorizedException.class)
    public void testRemoveDestinationNotAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);

        Subject subject = new PermsSubject();
        ConnectionContext context = createContext(subject);

        filter.removeDestination(context, dest, 1000);
    }

    @Test
    public void testRemoveDestinationAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);

        Subject subject = new PermsSubject() {
            @Override
            public boolean isPermitted(Permission toCheck) {
                Permission assigned = createPerm("topic:myTopic:remove");
                assertEquals(assigned.toString(), toCheck.toString());
                return assigned.implies(toCheck);
            }
        };

        ConnectionContext context = createContext(subject);

        filter.removeDestination(context, dest, 1000);
    }

    @Test(expected=UnauthorizedException.class)
    public void testAddConsumerNotAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);

        Subject subject = new PermsSubject();
        ConnectionContext context = createContext(subject);
        ConsumerInfo info = new ConsumerInfo(null);
        info.setDestination(dest);

        filter.addConsumer(context, info);
    }

    @Test
    public void testAddConsumerAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);

        Subject subject = new PermsSubject() {
            @Override
            public boolean isPermitted(Permission toCheck) {
                Permission assigned = createPerm("topic:myTopic:read");
                assertEquals(assigned.toString(), toCheck.toString());
                return assigned.implies(toCheck);
            }
        };

        ConnectionContext context = createContext(subject);
        ConsumerInfo info = new ConsumerInfo(null);
        info.setDestination(dest);

        filter.addConsumer(context, info);
    }

    @Test
    public void testAddProducerWithoutDestination() throws Exception {
        Subject subject = new PermsSubject();
        ConnectionContext context = createContext(subject);
        ProducerInfo info = new ProducerInfo(null);
        filter.addProducer(context, info);
    }

    @Test(expected=UnauthorizedException.class)
    public void testAddProducerNotAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);

        Subject subject = new PermsSubject();
        ConnectionContext context = createContext(subject);
        ProducerInfo info = new ProducerInfo(null);
        info.setDestination(dest);

        filter.addProducer(context, info);
    }

    @Test
    public void testAddProducerAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);

        Subject subject = new PermsSubject() {
            @Override
            public boolean isPermitted(Permission toCheck) {
                Permission assigned = createPerm("topic:myTopic:write");
                assertEquals(assigned.toString(), toCheck.toString());
                return assigned.implies(toCheck);
            }
        };

        ConnectionContext context = createContext(subject);
        ProducerInfo info = new ProducerInfo(null);
        info.setDestination(dest);

        filter.addProducer(context, info);
    }

    @Test(expected=UnauthorizedException.class)
    public void testBrokerExchangeSendNotAuthorized() throws Exception {
        String name = "myTopic";

        ActiveMQDestination dest = new ActiveMQTopic(name);
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setDestination(dest);
        message.setText("Hello, world!");

        Subject subject = new PermsSubject();
        ConnectionContext context = createContext(subject);
        ProducerBrokerExchange exchange = new ProducerBrokerExchange();
        exchange.setConnectionContext(context);

        filter.send(exchange, message);
    }

    @Test
    public void testBrokerExchangeSendAuthorized() throws Exception {
        String name = "myTopic";
        ActiveMQDestination dest = new ActiveMQTopic(name);
        ActiveMQTextMessage message = new ActiveMQTextMessage();
        message.setDestination(dest);
        message.setText("Hello, world!");

        Subject subject = new PermsSubject() {
            @Override
            public boolean isPermitted(Permission toCheck) {
                Permission assigned = createPerm("topic:myTopic:write");
                assertEquals(assigned.toString(), toCheck.toString());
                return assigned.implies(toCheck);
            }
        };

        ConnectionContext context = createContext(subject);
        ProducerBrokerExchange exchange = new ProducerBrokerExchange();
        exchange.setConnectionContext(context);

        filter.send(exchange, message);
    }


    protected Permission createPerm(String perm) {
        return new DestinationActionPermissionResolver().createPermission(perm);
    }


    private static class PermsSubject extends SubjectAdapter {

        @Override
        public PrincipalCollection getPrincipals() {
            return new SimplePrincipalCollection("foo", "someRealm");
        }

        @Override
        public boolean isPermittedAll(Collection<Permission> permissions) {
            assertNotNull(permissions);
            assertEquals(1, permissions.size());
            return isPermitted(permissions.iterator().next());
        }
    }
}
