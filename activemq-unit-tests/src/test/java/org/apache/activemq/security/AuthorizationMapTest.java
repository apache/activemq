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

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import junit.framework.TestCase;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.jaas.GroupPrincipal;

/**
 *
 *
 */
public class AuthorizationMapTest extends TestCase {
    static final GroupPrincipal USERS = new GroupPrincipal("users");
    static final GroupPrincipal ADMINS = new GroupPrincipal("admins");
    static final GroupPrincipal TEMP_DESTINATION_ADMINS = new GroupPrincipal("tempDestAdmins");

    public void testAuthorizationMap() {
        AuthorizationMap map = createAuthorizationMap();

        Set<?> readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.BAR"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));

    }

    public void testCompositeDoesNotBypassAuthorizationMap() {
        AuthorizationMap map = createAuthorizationMap();

        Set<?> readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.BAR,DENIED"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(ADMINS));
    }

    public void testAuthorizationMapWithTempDest() {
        AuthorizationMap map = createAuthorizationMapWithTempDest();

        Set<?> readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.BAR"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));

        Set<?> tempAdminACLs = map.getTempDestinationAdminACLs();
        assertEquals("set size: " + tempAdminACLs, 1, tempAdminACLs.size());
        assertTrue("Contains users group", tempAdminACLs.contains(TEMP_DESTINATION_ADMINS));

    }

    public void testWildcards() {
        AuthorizationMap map = createWildcardAuthorizationMap();

        Set<?> readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.BAR"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));

        Set<?> writeAcls = map.getWriteACLs(new ActiveMQQueue("USERS.FOO.BAR"));
        assertEquals("set size: " + writeAcls, 1, writeAcls.size());
        assertTrue("Contains users group", writeAcls.contains(ADMINS));
        assertTrue("Contains users group", writeAcls.contains(USERS));

        Set<?> adminAcls = map.getAdminACLs(new ActiveMQQueue("USERS.FOO.BAR"));
        assertEquals("set size: " + adminAcls, 1, adminAcls.size());
        assertTrue("Contains users group", adminAcls.contains(ADMINS));
        assertFalse("Contains users group", adminAcls.contains(USERS));

        Set<?> tempAdminACLs = map.getTempDestinationAdminACLs();
        assertEquals("set size: " + tempAdminACLs, 1, tempAdminACLs.size());
        assertTrue("Contains users group", tempAdminACLs.contains(TEMP_DESTINATION_ADMINS));
    }

    protected AuthorizationMap createWildcardAuthorizationMap() {
        DefaultAuthorizationMap answer = new DefaultAuthorizationMap();

        List<DestinationMapEntry> entries = new ArrayList<DestinationMapEntry>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setQueue(">");
        try {
            entry.setRead("*");
            entry.setWrite("*");
            entry.setAdmin("admins");
        } catch (Exception e) {
            fail(e.toString());
        }

        entries.add(entry);

        answer.setAuthorizationEntries(entries);

        TempDestinationAuthorizationEntry tEntry = new TempDestinationAuthorizationEntry();
        try {
            tEntry.setAdmin("*");
        } catch (Exception e) {
            fail(e.toString());
        }

        answer.setTempDestinationAuthorizationEntry(tEntry);

        return answer;

    }

    @SuppressWarnings("rawtypes")
    protected AuthorizationMap createAuthorizationMap() {
        DefaultAuthorizationMap answer = new DefaultAuthorizationMap();

        List<DestinationMapEntry> entries = new ArrayList<DestinationMapEntry>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setGroupClass("org.apache.activemq.jaas.GroupPrincipal");
        entry.setQueue(">");
        try {
            entry.setRead("admins");
        } catch (Exception e) {
            fail(e.toString());
        }

        entries.add(entry);
        // entry using default org.apache.activemq.jaas.GroupPrincipal class
        entry = new AuthorizationEntry();
        entry.setQueue("USERS.>");
        try {
            entry.setRead("users");
        } catch (Exception e) {
            fail(e.toString());
        }
        entries.add(entry);

        answer.setAuthorizationEntries(entries);

        return answer;
    }

    @SuppressWarnings("rawtypes")
    protected AuthorizationMap createAuthorizationMapWithTempDest() {
        DefaultAuthorizationMap answer = new DefaultAuthorizationMap();

        List<DestinationMapEntry> entries = new ArrayList<DestinationMapEntry>();

        AuthorizationEntry entry = new AuthorizationEntry();
        entry.setQueue(">");
        try {
            entry.setRead("admins");
        } catch (Exception e) {
            fail(e.toString());
        }
        entries.add(entry);

        entry = new AuthorizationEntry();
        entry.setQueue("USERS.>");
        try {
            entry.setRead("users");
        } catch (Exception e) {
            fail(e.toString());
        }
        entries.add(entry);

        answer.setAuthorizationEntries(entries);

        // create entry for temporary queue
        TempDestinationAuthorizationEntry tEntry = new TempDestinationAuthorizationEntry();
        try {
            tEntry.setAdmin("tempDestAdmins");
        } catch (Exception e) {
            fail(e.toString());
        }

        answer.setTempDestinationAuthorizationEntry(tEntry);

        return answer;
    }

}
