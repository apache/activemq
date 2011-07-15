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

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.ldap.client.api.message.BindResponse;
import org.apache.directory.ldap.client.api.message.ModifyDnResponse;
import org.apache.directory.ldap.client.api.message.ModifyRequest;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.apache.directory.server.core.integ.FrameworkRunner;
import org.apache.directory.shared.ldap.ldif.LdifEntry;
import org.apache.directory.shared.ldap.ldif.LdifReader;
import org.apache.directory.shared.ldap.message.ResultCodeEnum;
import org.apache.directory.shared.ldap.name.DN;
import org.apache.directory.shared.ldap.name.RDN;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;




@RunWith( FrameworkRunner.class )
@CreateLdapServer(transports = {@CreateTransport(protocol = "LDAP")})
@ApplyLdifFiles(
        "org/apache/activemq/security/activemq-apacheds.ldif"
)
public class CachedLDAPAuthorizationModuleTest extends AbstractLdapTestUnit {

    static final GroupPrincipal GUESTS = new GroupPrincipal("guests");
    static final GroupPrincipal USERS = new GroupPrincipal("users");
    static final GroupPrincipal ADMINS = new GroupPrincipal("admins");

    @Test
    public void testQuery() throws Exception {
        CachedLDAPAuthorizationMap map = new CachedLDAPAuthorizationMap();
        map.query();
        Set readACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains admin group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));

        Set failedACLs = map.getReadACLs(new ActiveMQQueue("FAILED"));
        assertEquals("set size: " + failedACLs, 0, failedACLs.size());
    }


    @Test
    public void testWildcards() throws Exception {
        CachedLDAPAuthorizationMap map1 = new CachedLDAPAuthorizationMap();
        map1.query();
        Set fooACLs = map1.getReadACLs(new ActiveMQQueue("FOO.1"));
        assertEquals("set size: " + fooACLs, 2, fooACLs.size());
        assertTrue("Contains admin group", fooACLs.contains(ADMINS));
        assertTrue("Contains users group", fooACLs.contains(USERS));

        CachedLDAPAuthorizationMap map2 = new CachedLDAPAuthorizationMap();
        map2.query();
        Set barACLs = map2.getReadACLs(new ActiveMQQueue("BAR.2"));
        assertEquals("set size: " + barACLs, 2, barACLs.size());
        assertTrue("Contains admin group", barACLs.contains(ADMINS));
        assertTrue("Contains users group", barACLs.contains(USERS));
    }

    @Test
    public void testAdvisory() throws Exception {
        CachedLDAPAuthorizationMap map = new CachedLDAPAuthorizationMap();
        map.query();
        Set readACLs = map.getReadACLs(new ActiveMQTopic("ActiveMQ.Advisory.Connection"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains admin group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));
    }

    @Test
    public void testTemporary() throws Exception {
        CachedLDAPAuthorizationMap map = new CachedLDAPAuthorizationMap();
        map.query();
        Thread.sleep(1000);
        Set readACLs = map.getTempDestinationReadACLs();
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains admin group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));
    }

    @Test
    public void testAdd() throws Exception {
        CachedLDAPAuthorizationMap map = new CachedLDAPAuthorizationMap();
        map.query();

        Set failedACLs = map.getReadACLs(new ActiveMQQueue("FAILED"));
        assertEquals("set size: " + failedACLs, 0, failedACLs.size());

        LdapConnection connection = new LdapConnection( "localhost", 1024 );
        BindResponse bindResponse = connection.bind("uid=admin,ou=system", "secret");
        assertNotNull(bindResponse);
        assertEquals(ResultCodeEnum.SUCCESS, bindResponse.getLdapResult().getResultCode());
        assertTrue(connection.isAuthenticated());


        LdifReader reader = new LdifReader(getClass().getClassLoader().getResourceAsStream("org/apache/activemq/security/add.ldif"));

        List<LdifEntry> entries = service.getTestEntries();
        for (LdifEntry entry : reader) {
            connection.add(entry.getEntry());

        }

        Thread.sleep(2000);

        failedACLs = map.getReadACLs(new ActiveMQQueue("FAILED"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());

        connection.close();


    }

    @Test
    public void testRemove() throws Exception {
        CachedLDAPAuthorizationMap map = new CachedLDAPAuthorizationMap();
        map.query();

        Set failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());

        LdapConnection connection = new LdapConnection( "localhost", 1024 );
        BindResponse bindResponse = connection.bind("uid=admin,ou=system", "secret");
        assertNotNull(bindResponse);
        assertEquals(ResultCodeEnum.SUCCESS, bindResponse.getLdapResult().getResultCode());
        assertTrue(connection.isAuthenticated());


        LdifReader reader = new LdifReader(getClass().getClassLoader().getResourceAsStream("org/apache/activemq/security/delete.ldif"));

        List<LdifEntry> entries = service.getTestEntries();
        for (LdifEntry entry : reader) {
            connection.delete(entry.getDn());
        }

        Thread.sleep(2000);

        failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 0, failedACLs.size());

        connection.close();
    }

    @Test
    public void testRename() throws Exception {
        CachedLDAPAuthorizationMap map = new CachedLDAPAuthorizationMap();
        map.query();

        Set failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());

        LdapConnection connection = new LdapConnection( "localhost", 1024 );
        BindResponse bindResponse = connection.bind("uid=admin,ou=system", "secret");
        assertNotNull(bindResponse);
        assertEquals(ResultCodeEnum.SUCCESS, bindResponse.getLdapResult().getResultCode());
        assertTrue(connection.isAuthenticated());

        ModifyDnResponse resp = connection.rename(new DN("cn=TEST.FOO,ou=Queue,ou=Destination,ou=ActiveMQ,ou=system"),
                new RDN("cn=TEST.BAR"));

        Thread.sleep(2000);

        failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 0, failedACLs.size());


        failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.BAR"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());

        connection.close();
    }

    @Test
    public void testChange() throws Exception {
        CachedLDAPAuthorizationMap map = new CachedLDAPAuthorizationMap();
        map.query();

        Set failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());

        LdapConnection connection = new LdapConnection( "localhost", 1024 );
        BindResponse bindResponse = connection.bind("uid=admin,ou=system", "secret");
        assertNotNull(bindResponse);
        assertEquals(ResultCodeEnum.SUCCESS, bindResponse.getLdapResult().getResultCode());
        assertTrue(connection.isAuthenticated());

        DN dn = new DN("cn=read,cn=TEST.FOO,ou=Queue,ou=Destination,ou=ActiveMQ,ou=system");

        ModifyRequest request = new ModifyRequest(dn);
        request.remove("member", "cn=users");

        connection.modify(request);

        Thread.sleep(2000);

        failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 1, failedACLs.size());

        connection.close();
    }

}

