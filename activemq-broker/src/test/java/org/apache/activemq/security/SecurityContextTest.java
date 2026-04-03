/*
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
package org.apache.activemq.security;

import static org.apache.activemq.security.SecurityContextTest.TestPrincipal.testPrincipal;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.security.Principal;
import java.util.Objects;
import java.util.Set;
import org.junit.Test;

public class SecurityContextTest {

    @Test
    public void testIsOneOf() {
        SecurityContext context = newContext(Set.of(testPrincipal("one"),
                testPrincipal("two"), testPrincipal("three")));

        // test valid combos
        assertTrue(context.isInOneOf(Set.of(testPrincipal("one"))));
        assertTrue(context.isInOneOf(Set.of(testPrincipal("two"))));
        assertTrue(context.isInOneOf(Set.of(testPrincipal("three"))));
        assertTrue(context.isInOneOf(Set.of(testPrincipal("three"),
                testPrincipal("four"), testPrincipal("five"))));

        // test no matching
        assertFalse(context.isInOneOf(Set.of(testPrincipal("four"),
                testPrincipal("five"))));
        assertFalse(context.isInOneOf(Set.of()));
        // different impl types, should not find
        assertFalse(context.isInOneOf(Set.of((Principal) () -> "one")));

        // empty set
        context = newContext(Set.of());
        assertFalse(context.isInOneOf(Set.of(testPrincipal("one"))));
        assertFalse(context.isInOneOf(Set.of()));
    }

    private SecurityContext newContext(Set<Principal> principals) {
        return new SecurityContext("user") {
            @Override
            public Set<Principal> getPrincipals() {
                return principals;
            }
        };
    }

    static class TestPrincipal implements Principal {
        private final String name;

        private TestPrincipal(String name) {
            this.name = name;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public boolean equals(Object o) {
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            TestPrincipal that = (TestPrincipal) o;
            return Objects.equals(name, that.name);
        }

        @Override
        public int hashCode() {
            return Objects.hashCode(name);
        }

        static TestPrincipal testPrincipal(String name) {
            return new TestPrincipal(name);
        }
    }

}
