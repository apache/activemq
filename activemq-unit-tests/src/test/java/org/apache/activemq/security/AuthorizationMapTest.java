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

    public void testComposite() {
        AuthorizationMap map = createAuthorizationMap();
        addABEntry(map);

        Set<?> readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.BAR,DENIED"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(ADMINS));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.BAR,USERS.BAR.FOO"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));

        readACLs = map.getReadACLs(new ActiveMQQueue("QUEUEA,QUEUEB"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));
    }

    protected void addABEntry(AuthorizationMap map) {
        DefaultAuthorizationMap defaultMap = (DefaultAuthorizationMap) map;
        defaultMap.put(new ActiveMQQueue("QUEUEA"), createEntry("QUEUEA", "users", "users", "users"));
        defaultMap.put(new ActiveMQQueue("QUEUEB"), createEntry("QUEUEB", "users", "users", "users"));
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

    public void testWildcardSubscriptions() {
        final GroupPrincipal USERSA = new GroupPrincipal("usersA");

        DefaultAuthorizationMap map = new DefaultAuthorizationMap();
        List<DestinationMapEntry> entries = new ArrayList<>();
        entries.add(createEntry("A", "usersA", null, null));
        map.setAuthorizationEntries(entries);

        Set<?> readACLs = map.getReadACLs(new ActiveMQQueue(">"));
        assertEquals("set size: " + readACLs, 0, readACLs.size());

        readACLs = map.getReadACLs(new ActiveMQQueue("A"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERSA));

        entries.add(createEntry("USERS.>", "users", null, null));
        map.setAuthorizationEntries(entries);

        readACLs = map.getReadACLs(new ActiveMQQueue(">"));
        assertEquals("set size: " + readACLs, 0, readACLs.size());

        readACLs = map.getReadACLs(new ActiveMQQueue("A"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERSA));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.>"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.>"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.TEST"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));

        entries.add(createEntry("USERS.A.>", "usersA", null, null));
        map.setAuthorizationEntries(entries);

        readACLs = map.getReadACLs(new ActiveMQQueue(">"));
        assertEquals("set size: " + readACLs, 0, readACLs.size());

        readACLs = map.getReadACLs(new ActiveMQQueue("A"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERSA));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.>"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.>"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.TEST"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.A.>"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));
        assertTrue("Contains users group", readACLs.contains(USERSA));

        entries.add(createEntry(">", "admins", null, null));
        map.setAuthorizationEntries(entries);

        readACLs = map.getReadACLs(new ActiveMQQueue(">"));
        assertEquals("set size: " + readACLs, 1, readACLs.size());
        assertTrue("Contains admins group", readACLs.contains(ADMINS));

        readACLs = map.getReadACLs(new ActiveMQQueue("A"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERSA));
        assertTrue("Contains admins group", readACLs.contains(ADMINS));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.>"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));
        assertTrue("Contains admins group", readACLs.contains(ADMINS));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.>"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains admins group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.TEST"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));
        assertTrue("Contains admins group", readACLs.contains(ADMINS));

        readACLs = map.getReadACLs(new ActiveMQQueue("USERS.A.>"));
        assertEquals("set size: " + readACLs, 3, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(USERS));
        assertTrue("Contains users group", readACLs.contains(USERSA));
        assertTrue("Contains admins group", readACLs.contains(ADMINS));
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

    protected AuthorizationEntry createEntry(String queue, String read, String write, String admin) {
        AuthorizationEntry entry = new AuthorizationEntry();
        if (queue != null) {
            entry.setQueue(queue);
        }
        try {
            if (read != null) {
                entry.setRead(read);
            }
            if (write != null) {
                entry.setWrite(write);
            }
            if (admin != null) {
                entry.setAdmin(admin);
            }
        } catch (Exception e) {
            fail(e.toString());
        }
        return entry;
    }

}
