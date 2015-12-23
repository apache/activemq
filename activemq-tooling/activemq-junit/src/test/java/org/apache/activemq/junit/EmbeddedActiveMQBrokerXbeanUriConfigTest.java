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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Verify the xbean configuration URI is working properly
 */
public class EmbeddedActiveMQBrokerXbeanUriConfigTest {

    @Rule
    public EmbeddedActiveMQBroker instance = new EmbeddedActiveMQBroker( "xbean:activemq-simple.xml");

    @Test
    public void testGetVmURL() throws Exception {
        assertEquals( "Default VM URL in incorrect", "failover:(vm://embedded-broker?create=false)", instance.getVmURL());
    }

    @Test
    public void testGetBrokerName() throws Exception {
        assertEquals( "Default Broker Name in incorrect", "embedded-broker", instance.getBrokerName());
    }

    @Test
    public void testBrokerNameConfig() throws Exception {
        String dummyName = "test-broker-name";

        instance.setBrokerName( dummyName);

        assertEquals( "Broker Name not set correctly", dummyName, instance.getBrokerName());
    }

    @Test
    public void testStatisticsPluginConfig() throws Exception {
        assertFalse( "Statistics plugin should not be enabled by default", instance.isStatisticsPluginEnabled());
        instance.enableStatisticsPlugin();
        assertTrue( "Statistics plugin not enabled", instance.isStatisticsPluginEnabled());
        instance.disableStatisticsPlugin();
        assertFalse( "Statistics plugin not disabled", instance.isStatisticsPluginEnabled());
    }

    @Test
    public void testAdvisoryForDeliveryConfig() throws Exception {
        assertFalse( "Advisory messages for delivery should not be enabled by default", instance.isAdvisoryForDeliveryEnabled());
        instance.enableAdvisoryForDelivery();
        assertTrue( "Advisory messages for delivery not enabled", instance.isAdvisoryForDeliveryEnabled());
        instance.disableAdvisoryForDelivery();
        assertFalse( "Advisory messages for delivery not disabled", instance.isAdvisoryForDeliveryEnabled());
    }

    @Test
    public void testAdvisoryForConsumedConfig() throws Exception {
        assertFalse( "Advisory messages for consumed should not be enabled by default", instance.isAdvisoryForConsumedEnabled());
        instance.enableAdvisoryForConsumed();
        assertTrue( "Advisory messages for consumed not enabled", instance.isAdvisoryForConsumedEnabled());
        instance.disableAdvisoryForConsumed();
        assertFalse( "Advisory messages for consumed not disabled", instance.isAdvisoryForConsumedEnabled());
    }

    @Test
    public void testAdvisoryForDiscardingMessagesConfig() throws Exception {
        assertFalse( "Advisory messages for discarding messages should not be enabled by default", instance.isAdvisoryForDiscardingMessagesEnabled());
        instance.enableAdvisoryForDiscardingMessages();
        assertTrue( "Advisory messages for discarding messages not enabled", instance.isAdvisoryForDiscardingMessagesEnabled());
        instance.disableAdvisoryForDiscardingMessages();
        assertFalse( "Advisory messages for discarding messages not disabled", instance.isAdvisoryForDiscardingMessagesEnabled());
    }

    @Test
    public void testAdvisoryForFastProducersConfig() throws Exception {
        assertFalse( "Advisory messages for fast producers should not be enabled by default", instance.isAdvisoryForFastProducersEnabled());
        instance.enableAdvisoryForFastProducers();
        assertTrue( "Advisory messages for fast producers not enabled", instance.isAdvisoryForFastProducersEnabled());
        instance.disableAdvisoryForFastProducers();
        assertFalse( "Advisory messages for fast producers not disabled", instance.isAdvisoryForFastProducersEnabled());
    }

    @Test
    public void testAdvisoryForSlowConsumersConfig() throws Exception {
        assertFalse( "Advisory messages for slow consumers should not be enabled by default", instance.isAdvisoryForSlowConsumersEnabled());
        instance.enableAdvisoryForSlowConsumers();
        assertTrue( "Advisory messages for slow consumers not enabled", instance.isAdvisoryForSlowConsumersEnabled());
        instance.disableAdvisoryForSlowConsumers();
        assertFalse( "Advisory messages for slow consumers not disabled", instance.isAdvisoryForSlowConsumersEnabled());
    }

}