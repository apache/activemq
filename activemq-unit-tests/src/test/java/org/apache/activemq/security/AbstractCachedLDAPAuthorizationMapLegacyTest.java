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
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import javax.naming.Context;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.util.Wait;
import org.apache.directory.api.ldap.model.name.Dn;
import org.apache.directory.api.ldap.model.name.Rdn;
import org.apache.directory.api.ldap.model.ldif.LdifEntry;
import org.apache.directory.api.ldap.model.ldif.LdifReader;
import org.apache.directory.api.ldap.model.message.ModifyRequest;
import org.apache.directory.api.ldap.model.message.ModifyRequestImpl;
import org.apache.directory.ldap.client.api.LdapConnection;
import org.apache.directory.server.core.integ.AbstractLdapTestUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractCachedLDAPAuthorizationMapLegacyTest extends AbstractLdapTestUnit {

    static final GroupPrincipal GUESTS = new GroupPrincipal("guests");
    static final GroupPrincipal USERS = new GroupPrincipal("users");
    static final GroupPrincipal ADMINS = new GroupPrincipal("admins");

    protected LdapConnection connection;
    protected SimpleCachedLDAPAuthorizationMap map;

    @Before
    public void setup() throws Exception {
        connection = getLdapConnection();
        map = createMap();
    }

    @After
    public void cleanup() throws Exception {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                // Ignore
            }
        }

        if (map != null) {
            map.destroy();
        }
    }

    @Test
    public void testQuery() throws Exception {
        map.query();
        Set<?> readACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains admin group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));

        Set<?> failedACLs = map.getReadACLs(new ActiveMQQueue("FAILED"));
        assertEquals("set size: " + failedACLs, 0, failedACLs.size());
    }

    @Test
    public void testSynchronousUpdate() throws Exception {
        map.setRefreshInterval(1);
        map.query();
        Set<?> readACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains admin group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));

        Set<?> failedACLs = map.getReadACLs(new ActiveMQQueue("FAILED"));
        assertEquals("set size: " + failedACLs, 0, failedACLs.size());

        LdifReader reader = new LdifReader(getRemoveLdif());

        for (LdifEntry entry : reader) {
            connection.delete(entry.getDn());
        }

        reader.close();

        assertTrue("did not get expected size. ", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return map.getReadACLs(new ActiveMQQueue("TEST.FOO")).size() == 0;
            }
        }));

        assertNull(map.getTempDestinationReadACLs());
        assertNull(map.getTempDestinationWriteACLs());
        assertNull(map.getTempDestinationAdminACLs());
    }

    @Test
    public void testWildcards() throws Exception {
        map.query();
        Set<?> fooACLs = map.getReadACLs(new ActiveMQQueue("FOO.1"));
        assertEquals("set size: " + fooACLs, 2, fooACLs.size());
        assertTrue("Contains admin group", fooACLs.contains(ADMINS));
        assertTrue("Contains users group", fooACLs.contains(USERS));

        Set<?> barACLs = map.getReadACLs(new ActiveMQQueue("BAR.2"));
        assertEquals("set size: " + barACLs, 2, barACLs.size());
        assertTrue("Contains admin group", barACLs.contains(ADMINS));
        assertTrue("Contains users group", barACLs.contains(USERS));
    }

    @Test
    public void testAdvisory() throws Exception {
        map.query();
        Set<?> readACLs = map.getReadACLs(new ActiveMQTopic("ActiveMQ.Advisory.Connection"));
        assertEquals("set size: " + readACLs, 2, readACLs.size());
        assertTrue("Contains admin group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));
    }

    @Test
    public void testTemporary() throws Exception {
        map.query();

        assertTrue("did not get expected temp ACLs", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                Set<?> acls = map.getTempDestinationReadACLs();
                return acls != null && acls.size() == 2;
            }
        }));
        Set<?> readACLs = map.getTempDestinationReadACLs();
        assertTrue("Contains admin group", readACLs.contains(ADMINS));
        assertTrue("Contains users group", readACLs.contains(USERS));
    }

    @Test
    public void testAdd() throws Exception {
        map.query();

        Set<?> failedACLs = map.getReadACLs(new ActiveMQQueue("FAILED"));
        assertEquals("set size: " + failedACLs, 0, failedACLs.size());

        LdifReader reader = new LdifReader(getAddLdif());

        for (LdifEntry entry : reader) {
            connection.add(entry.getEntry());
        }

        reader.close();

        assertTrue("did not get expected size after add", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return map.getReadACLs(new ActiveMQQueue("FAILED")).size() == 2;
            }
        }));
    }

    @Test
    public void testRemove() throws Exception {
        map.query();

        Set<?> failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());

        LdifReader reader = new LdifReader(getRemoveLdif());

        for (LdifEntry entry : reader) {
            connection.delete(entry.getDn());
        }

        reader.close();

        assertTrue("did not get expected size after remove", Wait.waitFor(
                () -> map.getReadACLs(new ActiveMQQueue("TEST.FOO")).size() == 0));

        // Temp destination ACLs are removed by a separate LDAP listener
        // (on the Temp subtree), so events may arrive after the Queue events.
        assertTrue("Temp read ACLs not cleared after remove", Wait.waitFor(() -> {
            final Set<?> acls = map.getTempDestinationReadACLs();
            return acls == null || acls.isEmpty();
        }));
        assertTrue("Temp write ACLs not cleared after remove", Wait.waitFor(() -> {
            final Set<?> acls = map.getTempDestinationWriteACLs();
            return acls == null || acls.isEmpty();
        }));
        assertTrue("Temp admin ACLs not cleared after remove", Wait.waitFor(() -> {
            final Set<?> acls = map.getTempDestinationAdminACLs();
            return acls == null || acls.isEmpty();
        }));
    }

    @Test
    public void testRenameDestination() throws Exception {
        map.query();

        // Test for a destination rename
        Set<?> failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());

        connection.rename(new Dn("cn=TEST.FOO," + getQueueBaseDn()),
                new Rdn("cn=TEST.BAR"));

        assertTrue("TEST.FOO ACLs not removed after rename", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return map.getReadACLs(new ActiveMQQueue("TEST.FOO")).size() == 0;
            }
        }));

        failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.BAR"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());
    }

    @Test
    public void testRenamePermission() throws Exception {
        map.query();

        // Test for a permission rename
        connection.delete(new Dn("cn=Read,cn=TEST.FOO," + getQueueBaseDn()));

        assertTrue("Read ACLs not removed after delete", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return map.getReadACLs(new ActiveMQQueue("TEST.FOO")).size() == 0;
            }
        }));

        Set<?> failedACLs = map.getWriteACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());

        connection.rename(new Dn("cn=Write,cn=TEST.FOO," + getQueueBaseDn()),
                new Rdn("cn=Read"));

        assertTrue("Read ACLs not restored after rename", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return map.getReadACLs(new ActiveMQQueue("TEST.FOO")).size() == 2;
            }
        }));

        failedACLs = map.getWriteACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 0, failedACLs.size());
    }

    @Test
    public void testChange() throws Exception {
        map.query();

        // Change permission entry
        Set<?> failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());

        Dn dn = new Dn("cn=read,cn=TEST.FOO," + getQueueBaseDn());

        ModifyRequest request = new ModifyRequestImpl();
        request.setName(dn);
        setupModifyRequest(request);

        connection.modify(request);

        assertTrue("Read ACLs not updated after modify", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return map.getReadACLs(new ActiveMQQueue("TEST.FOO")).size() == 1;
            }
        }));

        // Change destination entry
        request = new ModifyRequestImpl();
        request.setName(new Dn("cn=TEST.FOO," + getQueueBaseDn()));
        request.add("description", "This is a description!  In fact, it is a very good description.");

        connection.modify(request);

        assertTrue("Read ACLs changed unexpectedly after destination modify", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                return map.getReadACLs(new ActiveMQQueue("TEST.FOO")).size() == 1;
            }
        }));
    }

    @Test
    public void testRestartAsync() throws Exception {
        testRestart(false);
    }

    @Test
    public void testRestartSync() throws Exception {
        testRestart(true);
    }

    public void testRestart(final boolean sync) throws Exception {
        if (sync) {
            // ldap connection can be slow to close
            map.setRefreshInterval(1000);
        }
        map.query();

        Set<?> failedACLs = map.getReadACLs(new ActiveMQQueue("FAILED"));
        assertEquals("set size: " + failedACLs, 0, failedACLs.size());

        failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
        assertEquals("set size: " + failedACLs, 2, failedACLs.size());

        getLdapServer().stop();

        // wait for the context to be closed
        // as we can't rely on ldap server isStarted()
        assertTrue("Context was not closed after LDAP server stop", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                if (sync) {
                    return !map.isContextAlive();
                } else {
                    return map.context == null;
                }
            }
        }, 5*60*1000));

        // Verify cached ACLs are still available while LDAP is down.
        // Avoid calling getReadACLs here in async mode as it triggers
        // checkForUpdates which submits an async reconnection task to the
        // single-threaded updater.  That task blocks on the dead LDAP
        // server (TCP connect timeout), preventing later reconnection
        // tasks from executing and causing the test to time out.
        if (sync) {
            failedACLs = map.getReadACLs(new ActiveMQQueue("TEST.FOO"));
            assertEquals("set size: " + failedACLs, 2, failedACLs.size());
        }

        getLdapServer().start();

        // Wait for the LDAP server to be fully ready and force a synchronous
        // reconnection of the map, re-registering event listeners.
        assertTrue("Map did not reconnect to restarted LDAP server", Wait.waitFor(new Wait.Condition() {
            @Override
            public boolean isSatisified() throws Exception {
                try {
                    map.query();
                    return true;
                } catch (Exception e) {
                    return false;
                }
            }
        }));

        connection = getLdapConnection();

        LdifReader reader = new LdifReader(getAddLdif());

        for (LdifEntry entry : reader) {
            connection.add(entry.getEntry());
        }

        reader.close();

        assertTrue("did not get expected size. ", Wait.waitFor(new Wait.Condition() {

            @Override
            public boolean isSatisified() throws Exception {
                return map.getReadACLs(new ActiveMQQueue("FAILED")).size() == 2;
            }
        }));
    }

    protected SimpleCachedLDAPAuthorizationMap createMap() {
        return new SimpleCachedLDAPAuthorizationMap();
    }

    protected abstract InputStream getAddLdif();

    protected abstract InputStream getRemoveLdif();

    protected void setupModifyRequest(ModifyRequest request) {
        request.remove("member", "cn=users");
    }

    protected abstract String getQueueBaseDn();

    protected abstract LdapConnection getLdapConnection() throws Exception;

    public static void cleanAndLoad(String deleteFromDn, String ldifResourcePath,
            String ldapHost, int ldapPort, String ldapUser, String ldapPass,
            DirContext context) throws Exception {
        // Cleanup everything used for testing.
        List<String> dns = new LinkedList<String>();
        dns.add(deleteFromDn);

        while (!dns.isEmpty()) {
            String name = dns.get(dns.size() - 1);
            Context currentContext = (Context) context.lookup(name);
            NamingEnumeration<NameClassPair> namingEnum = currentContext.list("");

            if (namingEnum.hasMore()) {
                while (namingEnum.hasMore()) {
                    dns.add(namingEnum.next().getNameInNamespace());
                }
            } else {
                context.unbind(name);
                dns.remove(dns.size() - 1);
            }
        }

        // A bit of a hacked approach to loading an LDIF into OpenLDAP since there isn't an easy way to do it
        // otherwise.  This approach invokes the command line tool programmatically but has
        // to short-circuit the call to System.exit that the command line tool makes when it finishes.
        // We are assuming that there isn't already a security manager in place.
        final SecurityManager securityManager = new SecurityManager() {

            @Override
            public void checkPermission(java.security.Permission permission) {
                if (permission.getName().contains("exitVM")) {
                    throw new SecurityException("System.exit calls disabled for the moment.");
                }
            }
        };

        System.setSecurityManager(securityManager);


        File file = new File(AbstractCachedLDAPAuthorizationMapLegacyTest.class.getClassLoader().getResource(
                ldifResourcePath).toURI());

        Class<?> clazz = Class.forName("LDAPModify");
        Method mainMethod = clazz.getMethod("main", String[].class);

        try {
            mainMethod.invoke(null, new Object[] {
                    new String[] {
                            "-v",
                            "-h", ldapHost,
                            "-p", String.valueOf(ldapPort),
                            "-D", ldapUser,
                            "-w", ldapPass,
                            "-a",
                            "-f", file.toString()}});
        } catch (InvocationTargetException e) {
            if (!(e.getTargetException() instanceof SecurityException)) {
                throw e;
            }
        }

        System.setSecurityManager(null);
    }
}


