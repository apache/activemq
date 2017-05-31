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
package org.apache.activemq;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class AuthorizationTest extends AbstractAuthorizationTest {

    private static final int RECEIVE_TIMEOUT = 1000;
    String configurationSeed = "authorizationTest";

    @Test
    public void testMod() throws Exception {
        final String brokerConfig = configurationSeed + "-auth-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-users");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        assertAllowed("user", "USERS.A");
        assertDenied("user", "GUESTS.A");
        assertDenied("guest", "GUESTS.A");

        assertDeniedTemp("guest");

        applyNewConfig(brokerConfig, configurationSeed + "-users-guests", SLEEP);

        assertAllowed("user", "USERS.A");
        assertAllowed("guest", "GUESTS.A");
        assertDenied("user", "GUESTS.A");

        assertAllowedTemp("guest");
    }

    @Test
    public void testModRm() throws Exception {
        final String brokerConfig = configurationSeed + "-auth-rm-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-users-guests");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        assertAllowed("user", "USERS.A");
        assertAllowed("guest", "GUESTS.A");
        assertDenied("user", "GUESTS.A");
        assertAllowedTemp("guest");

        applyNewConfig(brokerConfig, configurationSeed + "-users", SLEEP);

        assertAllowed("user", "USERS.A");
        assertDenied("user", "GUESTS.A");
        assertDeniedTemp("guest");
    }

    @Test
    public void testModAddWrite() throws Exception {
        final String brokerConfig = configurationSeed + "-auth-rm-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-users");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        assertAllowedWrite("user", "USERS.A");
        assertDeniedWrite("guest", "USERS.A");

        applyNewConfig(brokerConfig, configurationSeed + "-users-add-write-guest", SLEEP);

        assertAllowedWrite("user", "USERS.A");
        assertAllowedWrite("guest", "USERS.A");
    }

    @Test
    public void testModWithGroupClass() throws Exception {
        final String brokerConfig = configurationSeed + "-auth-add-guest-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-users");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        assertAllowed("user", "USERS.A");
        applyNewConfig(brokerConfig, configurationSeed + "-users-dud-groupClass", SLEEP);
        assertDenied("user", "USERS.A");

        applyNewConfig(brokerConfig, configurationSeed + "-users", SLEEP);
        assertAllowed("user", "USERS.A");
    }

    @Test
    public void testWildcard() throws Exception {
        final String brokerConfig = configurationSeed + "-auth-broker";
        applyNewConfig(brokerConfig, configurationSeed + "-wildcard-users-guests");
        startBroker(brokerConfig);
        assertTrue("broker alive", brokerService.isStarted());

        final String ALL_USERS = "ALL.USERS.>";
        final String ALL_GUESTS = "ALL.GUESTS.>";

        assertAllowed("user", ALL_USERS);
        assertAllowed("guest", ALL_GUESTS);
        assertDenied("user", ALL_USERS + "," + ALL_GUESTS);
        assertDenied("guest", ALL_GUESTS + "," + ALL_USERS);

        final String ALL_PREFIX = "ALL.>";

        assertDenied("user", ALL_PREFIX);
        assertDenied("guest", ALL_PREFIX);

        assertAllowed("user", "ALL.USERS.A");
        assertAllowed("user", "ALL.USERS.A,ALL.USERS.B");
        assertAllowed("guest", "ALL.GUESTS.A");
        assertAllowed("guest", "ALL.GUESTS.A,ALL.GUESTS.B");

        assertDenied("user", "USERS.>");
        assertDenied("guest", "GUESTS.>");


        assertAllowedTemp("guest");
    }

}
