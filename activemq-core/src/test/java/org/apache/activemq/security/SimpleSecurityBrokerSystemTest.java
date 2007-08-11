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

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;

import junit.framework.Test;
import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.MessageSendTest;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.jaas.GroupPrincipal;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Tests that the broker allows/fails access to destinations based on the
 * security policy installed on the broker.
 * 
 * @version $Revision$
 */
public class SimpleSecurityBrokerSystemTest extends SecurityTestSupport {

    static final GroupPrincipal GUESTS = new GroupPrincipal("guests");
    static final GroupPrincipal USERS = new GroupPrincipal("users");
    static final GroupPrincipal ADMINS = new GroupPrincipal("admins");
    private static final Log LOG = LogFactory.getLog(SimpleSecurityBrokerSystemTest.class);

    public BrokerPlugin authorizationPlugin;
    public BrokerPlugin authenticationPlugin;

    static {
        String path = System.getProperty("java.security.auth.login.config");
        if (path == null) {
            URL resource = SimpleSecurityBrokerSystemTest.class.getClassLoader().getResource("login.config");
            if (resource != null) {
                path = resource.getFile();
                System.setProperty("java.security.auth.login.config", path);
            }
        }
        LOG.info("Path to login config: " + path);
    }

    public static Test suite() {
        return suite(SimpleSecurityBrokerSystemTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static AuthorizationMap createAuthorizationMap() {
        DestinationMap readAccess = new DestinationMap();
        readAccess.put(new ActiveMQQueue(">"), ADMINS);
        readAccess.put(new ActiveMQQueue("USERS.>"), USERS);
        readAccess.put(new ActiveMQQueue("GUEST.>"), GUESTS);
        readAccess.put(new ActiveMQTopic(">"), ADMINS);
        readAccess.put(new ActiveMQTopic("USERS.>"), USERS);
        readAccess.put(new ActiveMQTopic("GUEST.>"), GUESTS);

        DestinationMap writeAccess = new DestinationMap();
        writeAccess.put(new ActiveMQQueue(">"), ADMINS);
        writeAccess.put(new ActiveMQQueue("USERS.>"), USERS);
        writeAccess.put(new ActiveMQQueue("GUEST.>"), USERS);
        writeAccess.put(new ActiveMQQueue("GUEST.>"), GUESTS);
        writeAccess.put(new ActiveMQTopic(">"), ADMINS);
        writeAccess.put(new ActiveMQTopic("USERS.>"), USERS);
        writeAccess.put(new ActiveMQTopic("GUEST.>"), USERS);
        writeAccess.put(new ActiveMQTopic("GUEST.>"), GUESTS);

        readAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), GUESTS);
        readAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), USERS);
        writeAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), GUESTS);
        writeAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), USERS);

        DestinationMap adminAccess = new DestinationMap();
        adminAccess.put(new ActiveMQTopic(">"), ADMINS);
        adminAccess.put(new ActiveMQTopic(">"), USERS);
        adminAccess.put(new ActiveMQTopic(">"), GUESTS);
        adminAccess.put(new ActiveMQQueue(">"), ADMINS);
        adminAccess.put(new ActiveMQQueue(">"), USERS);
        adminAccess.put(new ActiveMQQueue(">"), GUESTS);

        return new SimpleAuthorizationMap(writeAccess, readAccess, adminAccess);
    }

    static class SimpleAuthenticationFactory implements BrokerPlugin {
        public Broker installPlugin(Broker broker) {

            HashMap<String, String> u = new HashMap<String, String>();
            u.put("system", "manager");
            u.put("user", "password");
            u.put("guest", "password");

            HashMap<String, HashSet<Object>> groups = new HashMap<String, HashSet<Object>>();
            groups.put("system", new HashSet<Object>(Arrays.asList(new Object[] {ADMINS, USERS})));
            groups.put("user", new HashSet<Object>(Arrays.asList(new Object[] {USERS})));
            groups.put("guest", new HashSet<Object>(Arrays.asList(new Object[] {GUESTS})));

            return new SimpleAuthenticationBroker(broker, u, groups);
        }

        public String toString() {
            return "SimpleAuthenticationBroker";
        }
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombos() {
        addCombinationValues("authorizationPlugin",
                             new Object[] {new AuthorizationPlugin(createAuthorizationMap())});
        addCombinationValues("authenticationPlugin", new Object[] {new SimpleAuthenticationFactory(),
                                                                   new JaasAuthenticationPlugin()});
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.setPlugins(new BrokerPlugin[] {authorizationPlugin, authenticationPlugin});
        broker.setPersistent(false);
        return broker;
    }

}
