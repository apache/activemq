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

import org.apache.shiro.authz.Permission;
import org.apache.shiro.authz.permission.WildcardPermission;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @since 5.10.0
 */
public class ActiveMQWildcardPermissionTest {

    @Test
    public void testNotWildcardPermission() {
        ActiveMQWildcardPermission perm = new ActiveMQWildcardPermission("topic:TEST:*");
        Permission dummy = new Permission() {
            @Override
            public boolean implies(Permission p) {
                return false;
            }
        };
       assertFalse(perm.implies(dummy));
    }

    @Test
    public void testIntrapartWildcard() {
        ActiveMQWildcardPermission superset = new ActiveMQWildcardPermission("topic:ActiveMQ.Advisory.*:read");
        ActiveMQWildcardPermission subset = new ActiveMQWildcardPermission("topic:ActiveMQ.Advisory.Topic:read");

        assertTrue(superset.implies(subset));
        assertFalse(subset.implies(superset));
    }

    @Test
    public void testMatches() {
        assertMatch("x", "x");
        assertNoMatch("x", "y");

        assertMatch("xx", "xx");
        assertNoMatch("xy", "xz");

        assertMatch("?", "x");
        assertMatch("x?", "xy");
        assertMatch("?y", "xy");
        assertMatch("x?z", "xyz");

        assertMatch("*", "x");
        assertMatch("x*", "x");
        assertMatch("x*", "xy");
        assertMatch("xy*", "xy");
        assertMatch("xy*", "xyz");

        assertMatch("*x", "x");
        assertNoMatch("*x", "y");

        assertMatch("*x", "wx");
        assertNoMatch("*x", "wz");
        assertMatch("*x", "vwx");

        assertMatch("x*z", "xz");
        assertMatch("x*z", "xyz");
        assertMatch("x*z", "xyyz");

        assertNoMatch("ab*t?z", "abz");
        assertNoMatch("ab*d*yz", "abcdz");

        assertMatch("ab**cd**ef*yz", "abcdefyz");
        assertMatch("a*c?*z", "abcxyz");
        assertMatch("a*cd*z", "abcdxyz");

        assertMatch("*", "x:x");
        assertMatch("*", "x:x:x");
        assertMatch("x", "x:y");
        assertMatch("x", "x:y:z");

        assertMatch("foo?armat*", "foobarmatches");
        assertMatch("f*", "f");
        assertNoMatch("foo", "f");
        assertMatch("fo*b", "foob");
        assertNoMatch("fo*b*r", "fooba");
        assertNoMatch("foo*", "f");

        assertMatch("t*k?ou", "thankyou");
        assertMatch("he*l*world", "helloworld");
        assertNoMatch("foo", "foob");

        assertMatch("*:ActiveMQ.Advisory", "foo:ActiveMQ.Advisory");
        assertNoMatch("*:ActiveMQ.Advisory", "foo:ActiveMQ.Advisory.");
        assertMatch("*:ActiveMQ.Advisory*", "foo:ActiveMQ.Advisory");
        assertMatch("*:ActiveMQ.Advisory*", "foo:ActiveMQ.Advisory.");
        assertMatch("*:ActiveMQ.Advisory.*", "foo:ActiveMQ.Advisory.Connection");
        assertMatch("*:ActiveMQ.Advisory*:read", "foo:ActiveMQ.Advisory.Connection:read");
        assertNoMatch("*:ActiveMQ.Advisory*:read", "foo:ActiveMQ.Advisory.Connection:write");
        assertMatch("*:ActiveMQ.Advisory*:*", "foo:ActiveMQ.Advisory.Connection:read");
        assertMatch("*:ActiveMQ.Advisory*:*", "foo:ActiveMQ.Advisory.");
        assertMatch("topic", "topic:TEST:*");
        assertNoMatch("*:ActiveMQ*", "topic:TEST:*");
        assertMatch("topic:ActiveMQ.Advisory*", "topic:ActiveMQ.Advisory.Connection:create");
        assertMatch("foo?ar", "foobar");
        
        assertMatch("queue:*:read,write", "queue:testqueue:read");
        assertMatch("queue:*:read,write", "queue:test*:read,write");
        assertNoMatch("queue:*:read,write", "queue:*:read,write,delete");
    }

    protected static void assertMatch(String pattern, String value) {
        assertTrue(matches(pattern, value));
    }

    protected static void assertNoMatch(String pattern, String value) {
        assertFalse(matches(pattern, value));
    }

    protected static boolean matches(String pattern, String value) {
        ActiveMQWildcardPermission patternPerm = new ActiveMQWildcardPermission(pattern);
        WildcardPermission valuePerm = new WildcardPermission(value, true);
        return patternPerm.implies(valuePerm);
    }

    @Test(expected=IllegalStateException.class)
    public void testGetPartsByReflectionThrowingException() {

        ActiveMQWildcardPermission perm = new ActiveMQWildcardPermission("foo:bar") {
            @Override
            protected List<Set<String>> doGetPartsByReflection(WildcardPermission wp) throws Exception {
                throw new RuntimeException("Testing failure");
            }
        };

        WildcardPermission otherPerm = new WildcardPermission("foo:bar:baz");

        perm.implies(otherPerm);
    }

    @Test
    public void testImpliesWithExtraParts() {
        ActiveMQWildcardPermission perm1 = new ActiveMQWildcardPermission("foo:bar:baz");
        ActiveMQWildcardPermission perm2 = new ActiveMQWildcardPermission("foo:bar");
        assertFalse(perm1.implies(perm2));
    }

}
