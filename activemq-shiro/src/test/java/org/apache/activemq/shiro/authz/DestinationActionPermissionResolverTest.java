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
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.AnyDestination;
import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.Before;
import org.junit.Test;

import java.util.Collection;

import static org.junit.Assert.*;

/**
 * @since 5.10.0
 */
public class DestinationActionPermissionResolverTest {

    private DestinationActionPermissionResolver resolver;

    @Before
    public void setUp() {
        this.resolver = new DestinationActionPermissionResolver();
    }

    @Test
    public void testDefaults() {
        assertNull(resolver.getPermissionStringPrefix());
        //default is true to reflect ActiveMQ's case-sensitive destination names:
        assertTrue(resolver.isPermissionStringCaseSensitive());
    }

    @Test
    public void testPermissionStringPrefixProp() {
        String prefix = "foo";
        resolver.setPermissionStringPrefix(prefix);
        assertEquals(prefix, resolver.getPermissionStringPrefix());
    }

    @Test
    public void testCaseSensitiveProp() {
        resolver.setPermissionStringCaseSensitive(true);
        assertTrue(resolver.isPermissionStringCaseSensitive());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPermissionsWithNonDestinationActionInstance() {
        resolver.getPermissions(new Action() {
            @Override
            public String toString() {
                return "foo";
            }
        });
    }

    @Test(expected = IllegalArgumentException.class)
    public void testGetPermissionsWithNullArgument() {
        resolver.getPermissions((Action)null);
    }

    void assertPermString(String perm, Collection<Permission> perms) {
        assertEquals(1, perms.size());
        assertEquals(perm, perms.iterator().next().toString());
    }

    @Test
    public void testGetPermissionsWithTopic() {
        ActiveMQTopic topic = new ActiveMQTopic("myTopic");
        DestinationAction action = new DestinationAction(new ConnectionContext(), topic, "create");
        Collection<Permission> perms = resolver.getPermissions(action);
        assertPermString("topic:myTopic:create", perms);
    }

    @Test
    public void testGetPermissionsWithTemporaryTopic() {
        ActiveMQTempTopic topic = new ActiveMQTempTopic("myTempTopic");
        DestinationAction action = new DestinationAction(new ConnectionContext(), topic, "remove");
        Collection<Permission> perms = resolver.getPermissions(action);
        assertPermString("temp-topic:myTempTopic:remove", perms);
    }

    @Test
    public void testGetPermissionsWithQueue() {
        ActiveMQQueue queue = new ActiveMQQueue("myQueue");
        DestinationAction action = new DestinationAction(new ConnectionContext(), queue, "write");
        Collection<Permission> perms = resolver.getPermissions(action);
        assertPermString("queue:myQueue:write", perms);
    }

    @Test
    public void testGetPermissionsWithTemporaryQueue() {
        ActiveMQTempQueue queue = new ActiveMQTempQueue("myTempQueue");
        DestinationAction action = new DestinationAction(new ConnectionContext(), queue, "read");
        Collection<Permission> perms = resolver.getPermissions(action);
        assertPermString("temp-queue:myTempQueue:read", perms);
    }

    @Test
    public void testPermissionWithPrefix() {
        resolver.setPermissionStringPrefix("activeMQ");
        ActiveMQTopic topic = new ActiveMQTopic("myTopic");
        DestinationAction action = new DestinationAction(new ConnectionContext(), topic, "create");
        Collection<Permission> perms = resolver.getPermissions(action);
        assertPermString("activeMQ:topic:myTopic:create", perms);
    }

    //Ensures if they explicitly set a prefix with a colon suffix that we don't add another one
    @Test
    public void testPermissionWithPrefixAndExplicitColon() {
        resolver.setPermissionStringPrefix("activeMQ:");
        ActiveMQTopic topic = new ActiveMQTopic("myTopic");
        DestinationAction action = new DestinationAction(new ConnectionContext(), topic, "create");
        Collection<Permission> perms = resolver.getPermissions(action);
        assertPermString("activeMQ:topic:myTopic:create", perms);
    }

    @Test
    public void testAlternateWildcardPermissionToStringWithMultipleActions() {
        Permission perm = resolver.createPermission("foo:bar:action1,action2");
        assertTrue(perm instanceof WildcardPermission);
        assertEquals("foo:bar:action1,action2", perm.toString());

    }

    @Test(expected = IllegalArgumentException.class)
    public void testCreatePermissionStringWithCompositeDestination() {
        ActiveMQTopic topicA = new ActiveMQTopic("A");
        ActiveMQTopic topicB = new ActiveMQTopic("B");
        ActiveMQDestination composite = new AnyDestination(new ActiveMQDestination[]{topicA, topicB});
        resolver.createPermissionString(composite, "read");
    }
}
