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

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Set;
import java.util.function.Function;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.broker.EmptyBroker;
import org.apache.activemq.broker.region.RegionBroker;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTempQueue;
import org.apache.activemq.command.ActiveMQTempTopic;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.activemq.jaas.UserPrincipal;
import org.junit.Before;
import org.junit.Test;

public class AuthorizationBrokerTest {

    private static final UserPrincipal USER1 = new UserPrincipal("user1");
    private static final UserPrincipal USER2 = new UserPrincipal("user2");
    private static final GroupPrincipal ADMINS = new GroupPrincipal("admins");

    private AuthorizationBroker broker;

    @Before
    public void setUp() throws Exception {
        BrokerService brokerService = new BrokerService();
        brokerService.setPersistent(false);
        brokerService.setUseJmx(false);
        brokerService.setAdvisorySupport(false);
        broker = new AuthorizationBroker(brokerService.getBroker(), createAuthorizationMap());
    }

    @Test
    public void testReadACLs() throws Exception {
        testAcls(broker::getReadACLs);
        testTempAcls(broker::getReadACLs);
        testAclsMixed(broker::getReadACLs);
    }

    @Test
    public void testWriteACLs() throws Exception {
        testAcls(broker::getWriteACLs);
        testTempAcls(broker::getWriteACLs);
        testAclsMixed(broker::getWriteACLs);
    }

    @Test
    public void testAdminACLs() throws Exception {
        testAcls(broker::getAdminACLs);
        testTempAcls(broker::getWriteACLs);
        testAclsMixed(broker::getWriteACLs);
    }
    
    private void testAcls(Function<ActiveMQDestination, Set<?>> acls) throws Exception {
        // Each user has access to their own dest
        assertEquals(Set.of(ADMINS, USER1), acls.apply(q("USER1.dest.1")));
        assertEquals(Set.of(ADMINS, USER1), acls.apply(t("USER1.dest.1")));
        assertEquals(Set.of(ADMINS, USER2), acls.apply(q("USER2.dest.1")));
        assertEquals(Set.of(ADMINS, USER2), acls.apply(t("USER2.dest.1")));

        // test composite, only admins should be authorized because each user only has permission
        // for one of the destinations
        assertEquals(Set.of(ADMINS), acls.apply(q("USER1.dest.1,USER2.dest.2")));
        assertEquals(Set.of(ADMINS), acls.apply(t("USER1.dest.1,USER2.dest.2")));
        // Composite should be authorized if all dests authorized for the user
        assertEquals(Set.of(ADMINS, USER2), acls.apply(q("USER2.dest.1,USER2.dest.2")));
        assertEquals(Set.of(ADMINS, USER2), acls.apply(t("USER2.dest.1,USER2.dest.2")));
    }

    private void testAclsMixed(Function<ActiveMQDestination, Set<?>> acls) throws Exception {
        // test composite, only admins should be authorized because each user only has permission
        // for one of the destinations, mix queue/topic
        assertEquals(Set.of(ADMINS), acls.apply(q("queue://USER1.dest.1,topic://USER2.dest.2")));
        assertEquals(Set.of(ADMINS), acls.apply(t("queue://USER1.dest.1,topic://USER2.dest.2")));

        // Composite should be authorized if all dests authorized for the user. Mix queue/topic
        assertEquals(Set.of(ADMINS, USER2), acls.apply(q("queue://USER2.dest.1,topic://USER2.dest.2")));
        assertEquals(Set.of(ADMINS, USER2), acls.apply(t("queue://USER2.dest.1,topic://USER2.dest.2")));

        // Composite temp - mix non-temp with top level temp - user can only access their own plus temp
        assertEquals(Set.of(ADMINS, USER1), acls.apply(tempQ("queue://USER1.dest.1,temp-topic://t.dest.2")));
        assertEquals(Set.of(ADMINS, USER1), acls.apply(tempT("topic://USER1.dest.1,temp-topic://t.dest.2")));

        // Composite temp - each dest is checked and users only have access to one of 2 so only admins are allowed
        assertEquals(Set.of(ADMINS), acls.apply(tempQ("queue://USER1.dest.1,topic://USER2.dest.1")));
        assertEquals(Set.of(ADMINS), acls.apply(tempT("topic://USER1.dest.1,topic://USER2.dest.1")));
    }

    private void testTempAcls(Function<ActiveMQDestination, Set<?>> acls) throws Exception {
        // These are temp destinations so users to have access to all temp by any name
        assertEquals(Set.of(ADMINS, USER1, USER2), acls.apply(tempQ("temp.dest.1")));
        assertEquals(Set.of(ADMINS, USER1, USER2), acls.apply(tempT("temp.dest.1")));
        assertEquals(Set.of(ADMINS, USER1, USER2), acls.apply(tempQ("temp.dest.2")));
        assertEquals(Set.of(ADMINS, USER1, USER2), acls.apply(tempT("temp.dest.2")));

        // test composite mixed, but these are temp so name doesn't matter
        assertEquals(Set.of(ADMINS, USER1, USER2), acls.apply(tempQ("temp.dest.1,temp.dest.2")));
        assertEquals(Set.of(ADMINS, USER1, USER2), acls.apply(tempT("temp.dest.1,temp.dest.2")));
        // Composite should be authorized if all temp topics
        assertEquals(Set.of(ADMINS, USER1, USER2), acls.apply(tempQ("t.dest.1,t.dest.2")));
        assertEquals(Set.of(ADMINS, USER1, USER2), acls.apply(tempT("t.dest.1,t.dest.2")));

        // Test multi-level - top level dests are all temp and allowed but child should only be
        // allowed for one
        ActiveMQDestination dest = tempQ("t.dest.1,t.dest.2");
        Arrays.stream(dest.getCompositeDestinations()).findFirst().orElseThrow().setCompositeDestinations(
                new ActiveMQDestination[]{new ActiveMQQueue("USER1.queue.1")});
        assertEquals(Set.of(ADMINS, USER1), acls.apply(dest));
        assertEquals(Set.of(ADMINS, USER1), acls.apply(dest));
    }

    private static AuthorizationMap createAuthorizationMap() throws Exception {
        // Use the same ACLs for read, write, admin to test all 3 behave the same
        var authorizationMap = new SimpleAuthorizationMap(createAclMap(), createAclMap(), createAclMap());
        var tempDestinationAuthorizationEntry = new TempDestinationAuthorizationEntry();
        tempDestinationAuthorizationEntry.setAdminACLs(Set.of(ADMINS, USER1, USER2));
        tempDestinationAuthorizationEntry.setReadACLs(Set.of(ADMINS, USER1, USER2));
        tempDestinationAuthorizationEntry.setWriteACLs(Set.of(ADMINS, USER1, USER2));
        authorizationMap.setTempDestinationAuthorizationEntry(tempDestinationAuthorizationEntry);
        return authorizationMap;
    }

    private static DestinationMap createAclMap() {
        DestinationMap access = new DefaultAuthorizationMap();
        access.put(new ActiveMQQueue(">"), ADMINS);
        access.put(new ActiveMQQueue("USER1.>"), USER1);
        access.put(new ActiveMQQueue("USER2.>"), USER2);
        access.put(new ActiveMQTopic(">"), ADMINS);
        access.put(new ActiveMQTopic("USER1.>"), USER1);
        access.put(new ActiveMQTopic("USER2.>"), USER2);
        return access;
    }

    private static ActiveMQQueue q(String queueName) {
        return new ActiveMQQueue(queueName);
    }

    private static ActiveMQTopic t(String topicName) {
        return new ActiveMQTopic(topicName);
    }

    private static ActiveMQTempQueue tempQ(String queueName) {
        return new ActiveMQTempQueue(queueName);
    }

    private static ActiveMQTempTopic tempT(String topicName) {
        return new ActiveMQTempTopic(topicName);
    }
}
