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
package org.apache.activemq.security;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.security.Principal;
import java.util.Arrays;
import java.util.Map;
import java.util.Set;

import org.junit.Test;

public class SimpleAuthenticationPluginTest {
    
    @Test
    public void testSetUsers() {
        AuthenticationUser alice = new AuthenticationUser("alice", "password", "group1");
        AuthenticationUser bob = new AuthenticationUser("bob", "security", "group2");
        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin();
        authenticationPlugin.setUsers(Arrays.asList(alice, bob));
        
        assertFalse(authenticationPlugin.isAnonymousAccessAllowed());
        
        Map<String, String> userPasswords = authenticationPlugin.getUserPasswords();
        assertEquals(2, userPasswords.size());
        assertEquals("password", userPasswords.get("alice"));
        assertEquals("security", userPasswords.get("bob"));
        
        Map<String, Set<Principal>> userGroups = authenticationPlugin.getUserGroups();
        assertEquals(2, userGroups.size());
        
        Set<Principal> aliceGroups = userGroups.get("alice");
        assertNotNull(aliceGroups);
        assertEquals(1, aliceGroups.size());
        assertEquals("group1", aliceGroups.iterator().next().getName());
        
        Set<Principal> bobGroups = userGroups.get("bob");
        assertNotNull(bobGroups);
        assertEquals(1, bobGroups.size());
        assertEquals("group2", bobGroups.iterator().next().getName());
    }
    
    @Test
    public void testSetUsersNoGroups() {
        AuthenticationUser alice = new AuthenticationUser("alice", "password", null);
        AuthenticationUser bob = new AuthenticationUser("bob", "security", null);
        SimpleAuthenticationPlugin authenticationPlugin = new SimpleAuthenticationPlugin();
        authenticationPlugin.setUsers(Arrays.asList(alice, bob));
        
        assertFalse(authenticationPlugin.isAnonymousAccessAllowed());
        
        Map<String, String> userPasswords = authenticationPlugin.getUserPasswords();
        assertEquals(2, userPasswords.size());
        assertEquals("password", userPasswords.get("alice"));
        assertEquals("security", userPasswords.get("bob"));
        
        Map<String, Set<Principal>> userGroups = authenticationPlugin.getUserGroups();
        assertEquals(2, userGroups.size());
        
        Set<Principal> aliceGroups = userGroups.get("alice");
        assertNotNull(aliceGroups);
        assertTrue(aliceGroups.isEmpty());
        
        Set<Principal> bobGroups = userGroups.get("bob");
        assertNotNull(bobGroups);
        assertTrue(bobGroups.isEmpty());
    }
}
