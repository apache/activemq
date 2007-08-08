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

import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.filter.DestinationMap;
import org.apache.activemq.jaas.GroupPrincipal;

import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

import junit.framework.Test;

/**
 * Tests that the broker allows/fails access to destinations based on the
 * security policy installed on the broker.
 * 
 * @version $Revision$
 */
public class SimpleSecurityBrokerSystemTest extends SecurityTestSupport {

    static final GroupPrincipal guests = new GroupPrincipal("guests");
    static final GroupPrincipal users = new GroupPrincipal("users");
    static final GroupPrincipal admins = new GroupPrincipal("admins");

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
        log.info("Path to login config: " + path);
    }

    public static Test suite() {
        return suite(SimpleSecurityBrokerSystemTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    public static AuthorizationMap createAuthorizationMap() {
        DestinationMap readAccess = new DestinationMap();
        readAccess.put(new ActiveMQQueue(">"), admins);
        readAccess.put(new ActiveMQQueue("USERS.>"), users);
        readAccess.put(new ActiveMQQueue("GUEST.>"), guests);
        readAccess.put(new ActiveMQTopic(">"), admins);
        readAccess.put(new ActiveMQTopic("USERS.>"), users);
        readAccess.put(new ActiveMQTopic("GUEST.>"), guests);

        DestinationMap writeAccess = new DestinationMap();
        writeAccess.put(new ActiveMQQueue(">"), admins);
        writeAccess.put(new ActiveMQQueue("USERS.>"), users);
        writeAccess.put(new ActiveMQQueue("GUEST.>"), users);
        writeAccess.put(new ActiveMQQueue("GUEST.>"), guests);
        writeAccess.put(new ActiveMQTopic(">"), admins);
        writeAccess.put(new ActiveMQTopic("USERS.>"), users);
        writeAccess.put(new ActiveMQTopic("GUEST.>"), users);
        writeAccess.put(new ActiveMQTopic("GUEST.>"), guests);

        readAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), guests);
        readAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), users);
        writeAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), guests);
        writeAccess.put(new ActiveMQTopic("ActiveMQ.Advisory.>"), users);

        DestinationMap adminAccess = new DestinationMap();
        adminAccess.put(new ActiveMQTopic(">"), admins);
        adminAccess.put(new ActiveMQTopic(">"), users);
        adminAccess.put(new ActiveMQTopic(">"), guests);
        adminAccess.put(new ActiveMQQueue(">"), admins);
        adminAccess.put(new ActiveMQQueue(">"), users);
        adminAccess.put(new ActiveMQQueue(">"), guests);

        return new SimpleAuthorizationMap(writeAccess, readAccess, adminAccess);
    }

    static class SimpleAuthenticationFactory implements BrokerPlugin {
        public Broker installPlugin(Broker broker) {

            HashMap u = new HashMap();
            u.put("system", "manager");
            u.put("user", "password");
            u.put("guest", "password");

            HashMap groups = new HashMap();
            groups.put("system", new HashSet(Arrays.asList(new Object[] { admins, users })));
            groups.put("user", new HashSet(Arrays.asList(new Object[] { users })));
            groups.put("guest", new HashSet(Arrays.asList(new Object[] { guests })));

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
        addCombinationValues("authorizationPlugin", new Object[] { new AuthorizationPlugin(createAuthorizationMap()), });
        addCombinationValues("authenticationPlugin", new Object[] { new SimpleAuthenticationFactory(), new JaasAuthenticationPlugin(), });
    }

    protected BrokerService createBroker() throws Exception {
        BrokerService broker = super.createBroker();
        broker.setPlugins(new BrokerPlugin[] { authorizationPlugin, authenticationPlugin });
        broker.setPersistent(false);
        return broker;
    }


}
