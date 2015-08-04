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
package org.apache.activemq.broker.jmx;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.filter.DestinationMapEntry;
import org.apache.activemq.security.AuthorizationEntry;
import org.apache.activemq.security.AuthorizationMap;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.DefaultAuthorizationMap;
import org.apache.activemq.security.JaasAuthenticationPlugin;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes sure a durable subscriber can be added and deleted from the
 * brokerServer.getAdminView() when JAAS authentication and authorization are
 * setup
 */
public class JMXRemoveDurableSubscriberTest {

    private static final Logger LOG = LoggerFactory.getLogger(JMXRemoveDurableSubscriberTest.class);

    private BrokerService brokerService;

    @SuppressWarnings("rawtypes")
    @Before
    public void setUp() throws Exception {
        brokerService = new BrokerService();

        JaasAuthenticationPlugin jaasAuthenticationPlugin = new JaasAuthenticationPlugin();
        jaasAuthenticationPlugin.setDiscoverLoginConfig(true);

        BrokerPlugin[] brokerPlugins = new BrokerPlugin[2];
        brokerPlugins[0] = jaasAuthenticationPlugin;

        AuthorizationPlugin authorizationPlugin = new AuthorizationPlugin();

        List<DestinationMapEntry> destinationMapEntries = new ArrayList<DestinationMapEntry>();

        // Add Authorization Entries.
        AuthorizationEntry authEntry1 = new AuthorizationEntry();
        authEntry1.setRead("manager,viewer,Operator,Maintainer,Deployer,Auditor,Administrator,SuperUser, admin");
        authEntry1.setWrite("manager,Operator,Maintainer,Deployer,Auditor,Administrator,SuperUser,admin");
        authEntry1.setAdmin("manager,Operator,Maintainer,Deployer,Auditor,Administrator,SuperUser,admin");
        authEntry1.setQueue(">");

        AuthorizationEntry authEntry2 = new AuthorizationEntry();
        authEntry2.setRead("manager,viewer,Operator,Maintainer,Deployer,Auditor,Administrator,SuperUser, admin");
        authEntry2.setWrite("manager,Operator,Maintainer,Deployer,Auditor,Administrator,SuperUser,admin");
        authEntry2.setAdmin("manager,Operator,Maintainer,Deployer,Auditor,Administrator,SuperUser,admin");
        authEntry2.setTopic(">");

        AuthorizationEntry authEntry3 = new AuthorizationEntry();
        authEntry3.setRead("manager,viewer,Operator,Maintainer,Deployer,Auditor,Administrator,SuperUser, admin");
        authEntry3.setWrite("manager,Operator,Maintainer,Deployer,Auditor,Administrator,SuperUser,admin");
        authEntry3.setAdmin("manager,Operator,Maintainer,Deployer,Auditor,Administrator,SuperUser,admin");
        authEntry3.setTopic("ActiveMQ.Advisory.>");

        destinationMapEntries.add(authEntry1);
        destinationMapEntries.add(authEntry2);
        destinationMapEntries.add(authEntry3);

        AuthorizationMap authorizationMap = new DefaultAuthorizationMap(destinationMapEntries);

        authorizationPlugin.setMap(authorizationMap);

        brokerPlugins[1] = authorizationPlugin;

        brokerService.setPlugins(brokerPlugins);

        brokerService.setBrokerName("ActiveMQBroker");
        brokerService.setPersistent(false);
        brokerService.setUseVirtualTopics(false);
        brokerService.setUseJmx(true);
        brokerService.addConnector("tcp://localhost:0");
        brokerService.start();
    }

    @After
    public void tearDown() throws Exception {
        if (brokerService != null) {
            try {
                brokerService.stop();
            } catch (Exception e) {
            }
        }
    }

    /**
     * Creates a durable subscription via the AdminView
     */
    @Test(timeout = 60000)
    public void testCreateDurableSubsciber() throws Exception {

        String clientId = "10";

        // Add a topic called test topic
        brokerService.getAdminView().addTopic("testTopic");

        boolean createSubscriberSecurityException = false;

        String subscriberName = "testSubscriber";

        // Create a durable subscriber with the name testSubscriber
        try {
            brokerService.getAdminView().createDurableSubscriber(clientId, subscriberName, "testTopic", null);
            LOG.info("Successfully created durable subscriber " + subscriberName + " via AdminView");
        } catch (java.lang.SecurityException se1) {
            if (se1.getMessage().equals("User is not authenticated.")) {
                createSubscriberSecurityException = true;
            }
        }
        assertFalse(createSubscriberSecurityException);

        // Delete the durable subscriber that was created earlier.
        boolean destroySubscriberSecurityException = false;
        try {
            brokerService.getAdminView().destroyDurableSubscriber(clientId, subscriberName);
            LOG.info("Successfully destroyed durable subscriber " + subscriberName + " via AdminView");
        } catch (java.lang.SecurityException se2) {
            if (se2.getMessage().equals("User is not authenticated.")) {
                destroySubscriberSecurityException = true;
            }
        }
        assertFalse(destroySubscriberSecurityException);

        // Just to make sure the subscriber was actually deleted, try deleting
        // the subscriber again
        // and that should throw an exception
        boolean subscriberAlreadyDeleted = false;
        try {
            brokerService.getAdminView().destroyDurableSubscriber(clientId, subscriberName);
            LOG.info("Successfully destroyed durable subscriber " + subscriberName + " via AdminView");
        } catch (javax.jms.InvalidDestinationException t) {
            if (t.getMessage().equals("No durable subscription exists for clientID: 10 and subscriptionName: testSubscriber")) {
                subscriberAlreadyDeleted = true;
            }
        }
        assertTrue(subscriberAlreadyDeleted);
    }
}
