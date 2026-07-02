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
package org.apache.activemq.store.kahadb;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;

import org.apache.activemq.store.MessageStoreStatistics;
import org.junit.Test;

/**
 * Tests for the MessageStoreStatisticsMarshaller, including
 * backward-compatible upgrade from the old 5-long format to the
 * new 6-long format that includes createdTimestamp.
 */
public class MessageStoreStatisticsMarshallerTest {

    private final MessageDatabase.MessageStoreStatisticsMarshaller marshaller =
            new MessageDatabase.MessageStoreStatisticsMarshaller();

    @Test
    public void testRoundTrip() throws Exception {
        MessageStoreStatistics original = new MessageStoreStatistics();
        original.getMessageCount().setCount(42);
        original.getMessageSize().setTotalSize(1024);
        original.getMessageSize().setMaxSize(256);
        original.getMessageSize().setMinSize(64);
        original.getMessageSize().setCount(10);
        original.setCreatedTimestamp(1719100000000L);

        byte[] data = write(original);
        MessageStoreStatistics restored = read(data);

        assertNotNull(restored);
        assertEquals(42, restored.getMessageCount().getCount());
        assertEquals(1024, restored.getMessageSize().getTotalSize());
        assertEquals(256, restored.getMessageSize().getMaxSize());
        assertEquals(64, restored.getMessageSize().getMinSize());
        assertEquals(10, restored.getMessageSize().getCount());
        assertEquals(1719100000000L, restored.getCreatedTimestamp());
    }

    @Test
    public void testNullRoundTrip() throws Exception {
        byte[] data = write(null);
        MessageStoreStatistics restored = read(data);
        assertNull(restored);
    }

    @Test
    public void testUpgradeFromOldFormatWithoutCreatedTimestamp() throws Exception {
        // Simulate the old 5-long format by writing without createdTimestamp
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);

        out.writeBoolean(true);
        out.writeLong(100);   // messageCount
        out.writeLong(5000);  // totalSize
        out.writeLong(500);   // maxSize
        out.writeLong(50);    // minSize
        out.writeLong(25);    // sizeCount

        byte[] oldFormatData = baos.toByteArray();

        // Read with the new marshaller — should handle missing createdTimestamp
        MessageStoreStatistics restored = read(oldFormatData);

        assertNotNull(restored);
        assertEquals(100, restored.getMessageCount().getCount());
        assertEquals(5000, restored.getMessageSize().getTotalSize());
        assertEquals(500, restored.getMessageSize().getMaxSize());
        assertEquals(50, restored.getMessageSize().getMinSize());
        assertEquals(25, restored.getMessageSize().getCount());
        assertEquals(0, restored.getCreatedTimestamp());
    }

    @Test
    public void testUpgradeThenRewrite() throws Exception {
        // Step 1: write in old 5-long format (no createdTimestamp)
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);

        out.writeBoolean(true);
        out.writeLong(100);
        out.writeLong(5000);
        out.writeLong(500);
        out.writeLong(50);
        out.writeLong(25);

        byte[] oldFormatData = baos.toByteArray();

        // Step 2: read with new marshaller
        MessageStoreStatistics restored = read(oldFormatData);
        assertNotNull(restored);
        assertEquals(0, restored.getCreatedTimestamp());

        // Step 3: set the timestamp and write back with new marshaller
        long now = System.currentTimeMillis();
        restored.setCreatedTimestamp(now);
        byte[] newFormatData = write(restored);

        // Step 4: read the new format — all fields including timestamp
        MessageStoreStatistics reloaded = read(newFormatData);
        assertNotNull(reloaded);
        assertEquals(100, reloaded.getMessageCount().getCount());
        assertEquals(5000, reloaded.getMessageSize().getTotalSize());
        assertEquals(500, reloaded.getMessageSize().getMaxSize());
        assertEquals(50, reloaded.getMessageSize().getMinSize());
        assertEquals(25, reloaded.getMessageSize().getCount());
        assertEquals(now, reloaded.getCreatedTimestamp());
    }

    @Test
    public void testNewFormatIsLargerThanOld() throws Exception {
        MessageStoreStatistics stats = new MessageStoreStatistics();
        stats.getMessageCount().setCount(1);
        stats.getMessageSize().setTotalSize(100);
        stats.getMessageSize().setMaxSize(100);
        stats.getMessageSize().setMinSize(100);
        stats.getMessageSize().setCount(1);
        stats.setCreatedTimestamp(System.currentTimeMillis());

        byte[] newFormat = write(stats);

        // Old format: 1 boolean + 5 longs = 1 + 40 = 41 bytes
        // New format: 1 boolean + 6 longs = 1 + 48 = 49 bytes
        assertEquals(49, newFormat.length);
    }

    @Test
    public void testCreatedTimestampOnNewDestination() throws Exception {
        long before = System.currentTimeMillis();

        MessageStoreStatistics stats = new MessageStoreStatistics();
        stats.setCreatedTimestamp(System.currentTimeMillis());

        long after = System.currentTimeMillis();

        byte[] data = write(stats);
        MessageStoreStatistics restored = read(data);

        assertTrue(restored.getCreatedTimestamp() >= before);
        assertTrue(restored.getCreatedTimestamp() <= after);
    }

    private byte[] write(MessageStoreStatistics stats) throws Exception {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutput out = new DataOutputStream(baos);
        marshaller.writePayload(stats, out);
        return baos.toByteArray();
    }

    private MessageStoreStatistics read(byte[] data) throws Exception {
        DataInput in = new DataInputStream(new ByteArrayInputStream(data));
        return marshaller.readPayload(in);
    }
}
