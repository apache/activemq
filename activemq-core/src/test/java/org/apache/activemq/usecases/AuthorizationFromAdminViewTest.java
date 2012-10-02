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
package org.apache.activemq.usecases;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.broker.BrokerFactory;
import org.apache.activemq.broker.BrokerPlugin;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.security.AuthorizationPlugin;
import org.apache.activemq.security.SimpleAuthorizationMap;

public class AuthorizationFromAdminViewTest extends org.apache.activemq.TestSupport {

    private BrokerService broker;

    protected ActiveMQConnectionFactory createConnectionFactory() throws Exception {
        return new ActiveMQConnectionFactory("vm://" + getName());
    }

    protected void setUp() throws Exception {
        createBroker();
        super.setUp();
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        destroyBroker();
    }

    private void createBroker() throws Exception {
        broker = BrokerFactory.createBroker("broker:(vm://localhost)");
        broker.setPersistent(false);
        broker.setBrokerName(getName());

        AuthorizationPlugin plugin = new AuthorizationPlugin();
        plugin.setMap(new SimpleAuthorizationMap());
        BrokerPlugin[] plugins = new BrokerPlugin[] {plugin};
        broker.setPlugins(plugins);

        broker.start();
    }

    private void destroyBroker() throws Exception {
        if (broker != null)
            broker.stop();
    }

    public void testAuthorizationFromAdminView() throws Exception {
        broker.getAdminView().addQueue(getDestinationString());
    }
}
