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
package org.apache.activemq.jaas;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import javax.naming.NamingEnumeration;
import javax.naming.directory.BasicAttributes;
import javax.naming.directory.DirContext;
import javax.naming.directory.SearchResult;
import javax.security.auth.Subject;

import org.junit.Test;

/**
 * Verifies that role expansion encodes the role name before it is placed into the LDAP
 * search filter. The role name used for expansion is the DN reported by the directory
 * (getNameInNamespace) and can contain characters that are special in an LDAP filter, so
 * it is encoded per RFC 2254 to keep the filter well formed.
 */
public class LDAPLoginModuleExpandRolesEncodingTest {

    // A role DN that contains characters that are special in an LDAP filter ( * ( ) ).
    private static final String ROLE_DN_WITH_SPECIAL_CHARS = "cn=team (a*b),ou=groups,ou=system";

    @Test
    public void testExpandRolesFilterIsEncoded() throws Exception {
        var capturedFilters = new ArrayList<String>();

        var module = new LDAPLoginModule();
        var options = new HashMap<String, String>();
        options.put("roleBase", "ou=groups,ou=system");
        options.put("roleName", "cn");
        options.put("roleSearchMatching", "(member=uid={1})");
        options.put("roleSearchSubtree", "true");
        options.put("expandRoles", "true");
        options.put("expandRolesMatching", "(member={0})");
        module.initialize(new Subject(), null, null, options);

        var context = capturingContext(capturedFilters, ROLE_DN_WITH_SPECIAL_CHARS);
        module.getRoles(context, "uid=first,ou=users,ou=system", "first", null);

        // First search is the role lookup, second is the expansion using the role DN.
        assertEquals(2, capturedFilters.size());
        var expandFilter = capturedFilters.get(1);

        assertFalse("expansion filter must not contain the unencoded role name",
                expandFilter.contains("a*b"));
        assertEquals("(member=" + module.doRFC2254Encoding(ROLE_DN_WITH_SPECIAL_CHARS) + ")",
                expandFilter);
    }

    // Returns a DirContext proxy that records the filter of every search call.
    // The first search yields one result whose in-namespace name is the role
    // DN, driving the expansion path; later searches return nothing.
    private static DirContext capturingContext(List<String> capturedFilters, String roleDn) {
        var attrs = new BasicAttributes();
        attrs.put("cn", "role");
        var result = new SearchResult("cn=role", null, attrs);
        result.setNameInNamespace(roleDn);

        int[] searchCount = {0};
        var handler = new InvocationHandler() {
            @Override
            public Object invoke(Object proxy, Method method, Object[] args) {
                switch (method.getName()) {
                case "search":
                    capturedFilters.add((String) args[1]);
                    var results = searchCount[0]++ == 0
                            ? Collections.singletonList(result)
                            : Collections.<SearchResult>emptyList();
                    return new ListNamingEnumeration(results);
                case "toString":
                    return "capturingContext";
                case "hashCode":
                    return System.identityHashCode(proxy);
                case "equals":
                    return proxy == args[0];
                default:
                    return null;
                }
            }
        };
        return (DirContext) Proxy.newProxyInstance(
                LDAPLoginModule.class.getClassLoader(),
                new Class[]{DirContext.class}, handler);
    }

    private static final class ListNamingEnumeration implements NamingEnumeration<SearchResult> {
        private final Iterator<SearchResult> iterator;

        ListNamingEnumeration(List<SearchResult> results) {
            this.iterator = results.iterator();
        }

        @Override
        public boolean hasMore() {
            return iterator.hasNext();
        }

        @Override
        public SearchResult next() {
            return iterator.next();
        }

        @Override
        public boolean hasMoreElements() {
            return iterator.hasNext();
        }

        @Override
        public SearchResult nextElement() {
            return iterator.next();
        }

        @Override
        public void close() {
        }
    }
}
