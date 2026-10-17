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
import static org.junit.Assert.assertNotNull;

import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Set;

import javax.naming.directory.DirContext;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.Rdn;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.test.annotations.ParallelTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * LDAPAuthorizationMap builds the LDAP search base by substituting the destination physical
 * name into a DN template. A name containing DN special characters (+, =, \, ", ;) must be
 * escaped per RFC 4514 so the assembled DN keeps the intended structure and the ACL lookup
 * resolves to the correct directory entry.
 *
 * Comma is the ActiveMQ composite-destination separator, so a comma splits the name into
 * separate destinations before the DN is built. These tests use + (an LDAP multi-valued RDN
 * separator) which reaches the DN construction intact.
 */
@Category(ParallelTest.class)
public class LDAPAuthorizationMapDestinationNameTest {

    static class CapturingLDAPAuthorizationMap extends LDAPAuthorizationMap {
        String capturedBase;

        @Override
        protected DirContext open() {
            return null; // no directory server needed; the search base is captured below
        }

        @Override
        protected Set<GroupPrincipal> getACLs(String destinationBase, SearchControls constraints,
                String roleBase, String roleAttribute) {
            this.capturedBase = destinationBase;
            return new HashSet<GroupPrincipal>();
        }
    }

    @Test
    public void queueDestinationNameIsEscapedInSearchBase() {
        var map = new CapturingLDAPAuthorizationMap();
        map.setQueueSearchMatchingFormat(
                new MessageFormat("uid={0},ou=queues,ou=destinations,o=ActiveMQ,ou=system"));

        var name = "orders+region=emea";
        map.getReadACLs(new ActiveMQQueue(name));

        assertNotNull(map.capturedBase);
        assertEquals("uid=" + Rdn.escapeValue(name) + ",ou=queues,ou=destinations,o=ActiveMQ,ou=system",
                map.capturedBase);
    }

    @Test
    public void topicDestinationNameIsEscapedInSearchBase() {
        var map = new CapturingLDAPAuthorizationMap();
        map.setTopicSearchMatchingFormat(
                new MessageFormat("uid={0},ou=topics,ou=destinations,o=ActiveMQ,ou=system"));

        var name = "orders+region=emea";
        map.getReadACLs(new ActiveMQTopic(name));

        assertNotNull(map.capturedBase);
        assertEquals("uid=" + Rdn.escapeValue(name) + ",ou=topics,ou=destinations,o=ActiveMQ,ou=system",
                map.capturedBase);
    }
}
