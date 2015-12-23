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
package org.apache.activemq.junit;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class MultipleEmbeddedActiveMQBrokerRuleTest {
    final String brokerOneName = "broker-one";
    final String brokerTwoName = "broker-two";

    @Rule
    public EmbeddedActiveMQBroker brokerOne = new EmbeddedActiveMQBroker();

    @Rule
    public EmbeddedActiveMQBroker brokerTwo = new EmbeddedActiveMQBroker();

    public MultipleEmbeddedActiveMQBrokerRuleTest() {
        // Perform and broker configuation here before JUnit starts the brokers
        brokerOne.setBrokerName( brokerOneName);
        brokerTwo.setBrokerName( brokerTwoName);
    }

    @Before
    public void setUp() throws Exception {
        assertTrue( "Broker One should be started", brokerOne.brokerService.isStarted());
        assertTrue( "Broker Two should be started", brokerTwo.brokerService.isStarted());
    }

    @After
    public void tearDown() throws Exception {
        assertTrue( "Broker One should still be running", brokerOne.brokerService.isStarted());
        assertTrue( "Broker Two should still be running", brokerTwo.brokerService.isStarted());
    }

    @Test
    public void testStart() throws Exception {
        assertEquals( "Broker One name is incorrect", brokerOneName, brokerOne.getBrokerName());
        assertEquals( "Broker Two name is incorrect", brokerTwoName, brokerTwo.getBrokerName());
    }
}