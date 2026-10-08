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
package org.apache.activemq.util;

import static org.junit.Assert.*;

import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.command.SubscriptionInfo;
import org.junit.Test;

public class SharedSubscriptionKeyTest {

    @Test
    public void testNullClientIdDoesNotThrow() {
        SharedSubscriptionKey key = new SharedSubscriptionKey(null, "mySub");
        assertEquals("", key.getClientId());
        assertEquals("mySub", key.getSubscriptionName());
    }

    @Test
    public void testSubscriptionNameOnlyConstructor() {
        SharedSubscriptionKey key = new SharedSubscriptionKey("mySub");
        assertEquals("", key.getClientId());
        assertEquals("mySub", key.getSubscriptionName());
    }

    @Test
    public void testWithClientId() {
        SharedSubscriptionKey key = new SharedSubscriptionKey("client1", "mySub");
        assertEquals("client1", key.getClientId());
        assertEquals("mySub", key.getSubscriptionName());
    }

    @Test
    public void testEqualsWithSameValues() {
        SharedSubscriptionKey key1 = new SharedSubscriptionKey("mySub");
        SharedSubscriptionKey key2 = new SharedSubscriptionKey("mySub");
        assertEquals(key1, key2);
        assertEquals(key1.hashCode(), key2.hashCode());
    }

    @Test
    public void testEqualsWithDifferentSubscriptions() {
        SharedSubscriptionKey key1 = new SharedSubscriptionKey("sub1");
        SharedSubscriptionKey key2 = new SharedSubscriptionKey("sub2");
        assertNotEquals(key1, key2);
    }

    @Test
    public void testEqualsWithParentSubscriptionKey() {
        SharedSubscriptionKey shared = new SharedSubscriptionKey("client1", "mySub");
        SubscriptionKey parent = new SubscriptionKey("client1", "mySub");
        assertEquals(shared, parent);
        assertEquals(parent, shared);
        assertEquals(shared.hashCode(), parent.hashCode());
    }

    @Test
    public void testWorksAsMapKey() {
        Map<SubscriptionKey, String> map = new HashMap<>();
        SharedSubscriptionKey key = new SharedSubscriptionKey("mySub");
        map.put(key, "value");

        assertEquals("value", map.get(key));
        assertEquals("value", map.get(new SharedSubscriptionKey("mySub")));
    }

    @Test
    public void testPolymorphicMapLookup() {
        Map<SubscriptionKey, String> map = new HashMap<>();
        SharedSubscriptionKey sharedKey = new SharedSubscriptionKey("client1", "mySub");
        map.put(sharedKey, "shared");

        SubscriptionKey parentKey = new SubscriptionKey("client1", "mySub");
        assertEquals("shared", map.get(parentKey));
    }

    @Test
    public void testToString() {
        SharedSubscriptionKey key = new SharedSubscriptionKey("mySub");
        assertEquals(":mySub", key.toString());
    }

    @Test
    public void testToStringWithClientId() {
        SharedSubscriptionKey key = new SharedSubscriptionKey("client1", "mySub");
        assertEquals("client1:mySub", key.toString());
    }

    @Test
    public void testFromSubscriptionInfo() {
        SubscriptionInfo info = new SubscriptionInfo("client1", "mySub");
        SharedSubscriptionKey key = new SharedSubscriptionKey(info);
        assertEquals("client1", key.getClientId());
        assertEquals("mySub", key.getSubscriptionName());
    }

    @Test
    public void testFromSubscriptionInfoNullClientId() {
        SubscriptionInfo info = new SubscriptionInfo(null, "mySub");
        SharedSubscriptionKey key = new SharedSubscriptionKey(info);
        assertEquals("", key.getClientId());
        assertEquals("mySub", key.getSubscriptionName());
    }

    @Test
    public void testIsInstanceOfSubscriptionKey() {
        SharedSubscriptionKey key = new SharedSubscriptionKey("mySub");
        assertTrue(key instanceof SubscriptionKey);
    }
}
