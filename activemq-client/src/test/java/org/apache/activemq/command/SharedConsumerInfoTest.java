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

public class SharedConsumerInfoTest {

    @Test
    public void testDefaultsToNonSharedNonDurable() {
        SharedConsumerInfo info = new SharedConsumerInfo();
        assertFalse(info.isShared());
        assertFalse(info.isDurable());
    }

    @Test
    public void testSharedFlag() {
        SharedConsumerInfo info = new SharedConsumerInfo();
        info.setShared(true);
        assertTrue(info.isShared());
    }

    @Test
    public void testDurableFlag() {
        SharedConsumerInfo info = new SharedConsumerInfo();
        info.setDurable(true);
        assertTrue(info.isDurable());
    }

    @Test
    public void testDurableOverridesParentInference() {
        SharedConsumerInfo info = new SharedConsumerInfo();
        info.setSubscriptionName("mySub");
        assertFalse(info.isDurable());

        info.setDurable(true);
        assertTrue(info.isDurable());
    }

    @Test
    public void testIsInstanceOfConsumerInfo() {
        SharedConsumerInfo info = new SharedConsumerInfo();
        assertTrue(info instanceof ConsumerInfo);
    }

    @Test
    public void testDataStructureType() {
        SharedConsumerInfo info = new SharedConsumerInfo();
        assertEquals(ConsumerInfo.DATA_STRUCTURE_TYPE, info.getDataStructureType());
    }

    @Test
    public void testCopyPreservesSharedFields() {
        SharedConsumerInfo original = new SharedConsumerInfo();
        original.setShared(true);
        original.setDurable(true);
        original.setSubscriptionName("mySub");

        SharedConsumerInfo copy = original.copy();
        assertTrue(copy.isShared());
        assertTrue(copy.isDurable());
        assertEquals("mySub", copy.getSubscriptionName());
    }

    @Test
    public void testCopyToParentConsumerInfo() {
        SharedConsumerInfo info = new SharedConsumerInfo();
        info.setShared(true);
        info.setSubscriptionName("mySub");

        ConsumerInfo parentCopy = new ConsumerInfo();
        info.copy(parentCopy);
        assertEquals("mySub", parentCopy.getSubscriptionName());
    }

    @Test
    public void testConstructorWithConsumerId() {
        ConsumerId cid = new ConsumerId(new SessionId(new ConnectionId("conn"), 1), 1);
        SharedConsumerInfo info = new SharedConsumerInfo(cid);
        assertEquals(cid, info.getConsumerId());
        assertFalse(info.isShared());
    }
}
