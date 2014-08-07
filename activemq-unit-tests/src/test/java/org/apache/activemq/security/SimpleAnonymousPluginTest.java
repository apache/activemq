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

import javax.jms.Connection;
import javax.jms.JMSException;

import junit.framework.Test;

import org.apache.activemq.CombinationTestSupport;
import org.apache.activemq.broker.BrokerService;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

public class SimpleAnonymousPluginTest extends SimpleAuthenticationPluginTest {

    public static Test suite() {
        return suite(SimpleAnonymousPluginTest.class);
    }

    public static void main(String[] args) {
        junit.textui.TestRunner.run(suite());
    }

    @Override
    protected BrokerService createBroker() throws Exception {
        return createBroker("org/apache/activemq/security/simple-anonymous-broker.xml");
    }

    @Override
    public void testInvalidAuthentication() throws JMSException {

        try {
            // Bad password
            Connection c = factory.createConnection("user", "krap");
            connections.add(c);
            c.start();
            fail("Expected exception.");
        } catch (JMSException e) {
        }

        try {
            // Bad userid
            Connection c = factory.createConnection("userkrap", null);
            connections.add(c);
            c.start();
            fail("Expected exception.");
        } catch (JMSException e) {
        }
    }

    public void testAnonymousReceiveSucceeds() throws JMSException {
        doReceive(false);
    }

    public void testAnonymousReceiveFails() throws JMSException {
        doReceive(true);
    }

    public void testAnonymousSendFails() throws JMSException {
        doSend(true);
    }

    public void testAnonymousSendSucceeds() throws JMSException {
        doSend(false);
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestAnonymousReceiveSucceeds() {
        addCombinationValues("userName", new Object[] { null });
        addCombinationValues("password", new Object[] { null });
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("GUEST.BAR"), new ActiveMQTopic("GUEST.BAR")});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestAnonymousReceiveFails() {
        addCombinationValues("userName", new Object[] { null });
        addCombinationValues("password", new Object[] { null });
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST"), new ActiveMQQueue("USERS.FOO"), new ActiveMQTopic("USERS.FOO") });
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestAnonymousSendFails() {
        addCombinationValues("userName", new Object[] { null });
        addCombinationValues("password", new Object[] { null });
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("TEST"), new ActiveMQTopic("TEST"), new ActiveMQQueue("USERS.FOO"), new ActiveMQTopic("USERS.FOO")});
    }

    /**
     * @see {@link CombinationTestSupport}
     */
    public void initCombosForTestAnonymousSendSucceeds() {
        addCombinationValues("userName", new Object[] { null });
        addCombinationValues("password", new Object[] { null });
        addCombinationValues("destination", new Object[] {new ActiveMQQueue("GUEST.BAR"), new ActiveMQTopic("GUEST.BAR")});
    }
}
