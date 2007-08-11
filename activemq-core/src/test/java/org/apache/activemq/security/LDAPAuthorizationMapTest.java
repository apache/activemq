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

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.naming.Context;
import javax.naming.NameClassPair;
import javax.naming.NamingEnumeration;
import javax.naming.directory.DirContext;
import javax.naming.directory.InitialDirContext;

import junit.framework.TestCase;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.directory.server.core.configuration.StartupConfiguration;
import org.apache.directory.server.core.jndi.CoreContextFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * This test assumes setup like in file 'AMQauth.ldif'. Contents of this file is
 * attached below in comments.
 * 
 * @author ngcutura
 * 
 */
public class LDAPAuthorizationMapTest extends TestCase {
    private LDAPAuthorizationMap authMap;

    protected void setUp() throws Exception {
        super.setUp();

        startLdapServer();

        authMap = new LDAPAuthorizationMap();
    }

    protected void startLdapServer() throws Exception {
        ApplicationContext factory = new ClassPathXmlApplicationContext("org/apache/activemq/security/ldap-spring.xml");
        StartupConfiguration cfg = (StartupConfiguration) factory.getBean("configuration");
        Properties env = (Properties) factory.getBean("environment");

        env.setProperty(Context.PROVIDER_URL, "");
        env.setProperty(Context.INITIAL_CONTEXT_FACTORY, CoreContextFactory.class.getName());
        env.putAll(cfg.toJndiEnvironment());

        new InitialDirContext(env);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
    }

    public void testOpen() throws Exception {
        DirContext ctx = authMap.open();
        HashSet<String> set = new HashSet<String>();
        NamingEnumeration list = ctx.list("ou=destinations,o=ActiveMQ,dc=example,dc=com");
        while (list.hasMore()) {
            NameClassPair ncp = (NameClassPair) list.next();
            set.add(ncp.getName());
        }
        assertTrue(set.contains("ou=topics"));
        assertTrue(set.contains("ou=queues"));
    }

    /*
     * Test method for
     * 'org.apache.activemq.security.LDAPAuthorizationMap.getAdminACLs(ActiveMQDestination)'
     */
    public void testGetAdminACLs() {
        ActiveMQDestination q1 = new ActiveMQQueue("queue1");
        Set aclsq1 = authMap.getAdminACLs(q1);
        assertEquals(1, aclsq1.size());
        assertTrue(aclsq1.contains(new GroupPrincipal("role1")));

        ActiveMQDestination t1 = new ActiveMQTopic("topic1");
        Set aclst1 = authMap.getAdminACLs(t1);
        assertEquals(1, aclst1.size());
        assertTrue(aclst1.contains(new GroupPrincipal("role1")));
    }

    /*
     * Test method for
     * 'org.apache.activemq.security.LDAPAuthorizationMap.getReadACLs(ActiveMQDestination)'
     */
    public void testGetReadACLs() {
        ActiveMQDestination q1 = new ActiveMQQueue("queue1");
        Set aclsq1 = authMap.getReadACLs(q1);
        assertEquals(1, aclsq1.size());
        assertTrue(aclsq1.contains(new GroupPrincipal("role1")));

        ActiveMQDestination t1 = new ActiveMQTopic("topic1");
        Set aclst1 = authMap.getReadACLs(t1);
        assertEquals(1, aclst1.size());
        assertTrue(aclst1.contains(new GroupPrincipal("role2")));
    }

    /*
     * Test method for
     * 'org.apache.activemq.security.LDAPAuthorizationMap.getWriteACLs(ActiveMQDestination)'
     */
    public void testGetWriteACLs() {
        ActiveMQDestination q1 = new ActiveMQQueue("queue1");
        Set aclsq1 = authMap.getWriteACLs(q1);
        assertEquals(2, aclsq1.size());
        assertTrue(aclsq1.contains(new GroupPrincipal("role1")));
        assertTrue(aclsq1.contains(new GroupPrincipal("role2")));

        ActiveMQDestination t1 = new ActiveMQTopic("topic1");
        Set aclst1 = authMap.getWriteACLs(t1);
        assertEquals(1, aclst1.size());
        assertTrue(aclst1.contains(new GroupPrincipal("role3")));
    }

}
