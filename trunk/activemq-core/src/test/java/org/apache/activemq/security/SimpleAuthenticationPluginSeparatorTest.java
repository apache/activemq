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

import junit.framework.Test;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

public class SimpleAuthenticationPluginSeparatorTest extends SimpleAuthenticationPluginTest {

    public static Test suite() {
        return suite(SimpleAuthenticationPluginSeparatorTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        return createBroker("org/apache/activemq/security/simple-auth-separator.xml");
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestUserReceiveFails() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST"), new ActiveMQQueue("GUEST/BAR"), new ActiveMQTopic("GUEST/BAR")});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestInvalidAuthentication() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestUserReceiveSucceeds() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("USERS/FOO"), new ActiveMQTopic("USERS/FOO")});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestGuestReceiveSucceeds() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("GUEST/BAR"), new ActiveMQTopic("GUEST/BAR")});
    }

    /**
     * @see {@link org.apache.activemq.CombinationTestSupport}
     */
    public void initCombosForTestGuestReceiveFails() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST"), new ActiveMQQueue("USERS/FOO"), new ActiveMQTopic("USERS/FOO") });
    }

    /**
     * @see {@link org.apache.activemq.CombinationTestSupport}
     */
    public void initCombosForTestUserSendSucceeds() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("USERS/FOO"), new ActiveMQQueue("GUEST/BAR"), new ActiveMQTopic("USERS/FOO"),
                                                          new ActiveMQTopic("GUEST/BAR")});
    }

    /**
     * @see {@link org.apache.activemq.CombinationTestSupport}
     */
    public void initCombosForTestUserSendFails() {
        addCombinationValues("userName", new Object[] {"user"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST")});
    }

    /**
     * @see {@link org.apache.activemq.CombinationTestSupport}
     */
    public void initCombosForTestGuestSendFails() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST"), new ActiveMQQueue("USERS/FOO"), new ActiveMQTopic("USERS/FOO")});
    }

    /**
     * @see {@link org.apache.activemq.CombinationTestSupport}
     */
    public void initCombosForTestGuestSendSucceeds() {
        addCombinationValues("userName", new Object[] {"guest"});
        addCombinationValues("password", new Object[] {"password"});
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("GUEST/BAR"), new ActiveMQTopic("GUEST/BAR")});
    }
}
