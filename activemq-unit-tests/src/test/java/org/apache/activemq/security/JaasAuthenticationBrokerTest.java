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

import javax.jms.InvalidClientIDException;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.concurrent.CopyOnWriteArrayList;

import junit.framework.TestCase;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.StubBroker;
import org.apache.activemq.command.ConnectionInfo;

public class JaasAuthenticationBrokerTest extends TestCase {
    StubBroker receiveBroker;

    JaasAuthenticationBroker authBroker;

    ConnectionContext connectionContext;
    ConnectionInfo connectionInfo;

    CopyOnWriteArrayList<SecurityContext> visibleSecurityContexts;

    class JaasAuthenticationBrokerTester extends JaasAuthenticationBroker {
        public JaasAuthenticationBrokerTester(Broker next, String jassConfiguration) {
            super(next, jassConfiguration);
            visibleSecurityContexts = securityContexts;
        }
    }

    @Override
    protected void setUp() throws Exception {
        receiveBroker = new StubBroker();

        authBroker = new JaasAuthenticationBrokerTester(receiveBroker, "");

        connectionContext = new ConnectionContext();
        connectionInfo = new ConnectionInfo();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
    }

    private void setConfiguration(boolean loginShouldSucceed) {
        HashMap<String, String> configOptions = new HashMap<String, String>();

        configOptions.put(StubLoginModule.ALLOW_LOGIN_PROPERTY, loginShouldSucceed ? "true" : "false");
        configOptions.put(StubLoginModule.USERS_PROPERTY, "");
        configOptions.put(StubLoginModule.GROUPS_PROPERTY, "");
        AppConfigurationEntry configEntry = new AppConfigurationEntry("org.apache.activemq.security.StubLoginModule", AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, configOptions);

        StubJaasConfiguration jaasConfig = new StubJaasConfiguration(configEntry);

        Configuration.setConfiguration(jaasConfig);
    }


    public void testAddConnectionFailureOnDuplicateClientId() throws Exception {
        setConfiguration(true);

        connectionInfo.setClientId("CliIdX");
        authBroker.addConnection(connectionContext, connectionInfo);
        ConnectionContext secondContext = connectionContext.copy();
        secondContext.setSecurityContext(null);
        ConnectionInfo secondInfo = connectionInfo.copy();
        try {
            authBroker.addConnection(secondContext, secondInfo);
            fail("Expect duplicate id");
        } catch (InvalidClientIDException expected) {
        }

        assertEquals("one connection allowed.", 1, receiveBroker.addConnectionData.size());
        assertEquals("one context .", 1, visibleSecurityContexts.size());

    }
}
