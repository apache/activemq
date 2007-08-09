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
import org.apache.activemq.jaas.GroupPrincipal;

/**
 * 
 * @version $Revision$
 */
public class AuthorizationMapTest extends TestCase {
    static final GroupPrincipal GUESTS = new GroupPrincipal("guests");
    static final GroupPrincipal USERS = new GroupPrincipal("users");
    static final GroupPrincipal ADMINS = new GroupPrincipal("admins");
    static final GroupPrincipal TEMP_DESTINATION_ADMINS = new GroupPrincipal("tempDestAdmins");

    public void testAuthorizationMap() {
        AuthorizationMap map = createAuthorizationMap();

        Set readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.BAR"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));

    }

    public void testAuthorizationMapWithTempDest() {
        AuthorizationMap map = createAuthorizationMapWithTempDest();

        Set readACLs = map.getReadACLs(new ActiveMQQueue("USERS.FOO.BAR"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains users group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));

        Set tempAdminACLs = map.getTempDestinationAdminACLs();
        assertEquals("set size: " + tempAdminACLs, 1, tempAdminACLs.size());
        assertTrue("Contains users group", tempAdminACLs.contains(TEMP_DESTINATION_ADMINS));

    }

    protected AuthorizationMap createAuthorizationMap() {
        DefaultAuthorizationMap answer = new DefaultAuthorizationMap();

        List entries = new ArrayList();

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

    protected AuthorizationMap createAuthorizationMapWithTempDest() {
        DefaultAuthorizationMap answer = new DefaultAuthorizationMap();

        List entries = new ArrayList();

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
