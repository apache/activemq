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
package org.apache.activemq.command;

import static org.junit.Assert.*;

import org.junit.Test;

public class SharedSubscriptionInfoTest {

    @Test
    public void testDefaultsToNonShared() {
        SharedSubscriptionInfo info = new SharedSubscriptionInfo();
        assertFalse(info.isShared());
    }

    @Test
    public void testSharedFlag() {
        SharedSubscriptionInfo info = new SharedSubscriptionInfo();
        info.setShared(true);
        assertTrue(info.isShared());
    }

    @Test
    public void testIsInstanceOfSubscriptionInfo() {
        SharedSubscriptionInfo info = new SharedSubscriptionInfo();
        assertTrue(info instanceof SubscriptionInfo);
    }

    @Test
    public void testDataStructureType() {
        SharedSubscriptionInfo info = new SharedSubscriptionInfo();
        assertEquals(SubscriptionInfo.DATA_STRUCTURE_TYPE, info.getDataStructureType());
    }

    @Test
    public void testConstructorWithClientIdAndName() {
        SharedSubscriptionInfo info = new SharedSubscriptionInfo("client1", "mySub");
        assertEquals("client1", info.getClientId());
        assertEquals("mySub", info.getSubscriptionName());
        assertFalse(info.isShared());
    }

    @Test
    public void testNullClientId() {
        SharedSubscriptionInfo info = new SharedSubscriptionInfo(null, "mySub");
        assertNull(info.getClientId());
        assertEquals("mySub", info.getSubscriptionName());
    }

    @Test
    public void testEqualsWithParent() {
        SharedSubscriptionInfo shared = new SharedSubscriptionInfo("client1", "mySub");
        shared.setShared(true);
        SubscriptionInfo plain = new SubscriptionInfo("client1", "mySub");

        assertTrue(plain.equals(shared));
        assertTrue(shared.equals(plain));
    }

    @Test
    public void testInheritsAllParentFields() {
        SharedSubscriptionInfo info = new SharedSubscriptionInfo();
        info.setClientId("client1");
        info.setSubscriptionName("mySub");
        info.setSelector("color = 'blue'");
        info.setDestination(new ActiveMQTopic("test.topic"));
        info.setNoLocal(true);
        info.setShared(true);

        assertEquals("client1", info.getClientId());
        assertEquals("mySub", info.getSubscriptionName());
        assertEquals("color = 'blue'", info.getSelector());
        assertTrue(info.isNoLocal());
        assertTrue(info.isShared());
    }
}
