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
package org.apache.activemq.transport.amqp.protocol;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;

import org.junit.Ignore;
import org.junit.Test;

/**
 * Tests for the AMQP Transfer Tag Generator
 */
public class AmqpTransferTagGeneratorTest {

    @Test
    public void testCreate() {
        AmqpTransferTagGenerator tagGen = new AmqpTransferTagGenerator();
        assertTrue(tagGen.isPooling());
        assertEquals(AmqpTransferTagGenerator.DEFAULT_TAG_POOL_SIZE, tagGen.getMaxPoolSize());
    }

    @Test
    public void testCreateDisabled() {
        AmqpTransferTagGenerator tagGen = new AmqpTransferTagGenerator(false);
        assertFalse(tagGen.isPooling());
        assertEquals(AmqpTransferTagGenerator.DEFAULT_TAG_POOL_SIZE, tagGen.getMaxPoolSize());
    }

    @Test
    public void testNewTagsOnSuccessiveCheckouts() {
        AmqpTransferTagGenerator tagGen = new AmqpTransferTagGenerator(true);

        byte[] tag1 = tagGen.getNextTag();
        byte[] tag2 = tagGen.getNextTag();
        byte[] tag3 = tagGen.getNextTag();

        assertNotSame(tag1, tag2);
        assertNotSame(tag1, tag3);
        assertNotSame(tag3, tag2);

        assertFalse(Arrays.equals(tag1, tag2));
        assertFalse(Arrays.equals(tag1, tag3));
        assertFalse(Arrays.equals(tag3, tag2));
    }

    @Test
    public void testTagPoolingInEffect() {
        AmqpTransferTagGenerator tagGen = new AmqpTransferTagGenerator(true);

        byte[] tag1 = tagGen.getNextTag();
        byte[] tag2 = tagGen.getNextTag();
        tagGen.returnTag(tag1);
        tagGen.returnTag(tag2);
        byte[] tag3 = tagGen.getNextTag();
        byte[] tag4 = tagGen.getNextTag();

        assertSame(tag1, tag3);
        assertSame(tag2, tag4);
        assertNotSame(tag1, tag4);
        assertNotSame(tag2, tag3);
    }

    @Test
    public void testPooledTagsReturnedInCheckedInOrder() {
        AmqpTransferTagGenerator tagGen = new AmqpTransferTagGenerator(true);

        byte[] tag1 = tagGen.getNextTag();
        byte[] tag2 = tagGen.getNextTag();
        tagGen.returnTag(tag2);
        tagGen.returnTag(tag1);
        byte[] tag3 = tagGen.getNextTag();
        byte[] tag4 = tagGen.getNextTag();

        assertSame(tag1, tag4);
        assertSame(tag2, tag3);
        assertNotSame(tag1, tag3);
        assertNotSame(tag2, tag4);
    }

    @Test
    public void testTagArrayGrowsWithTagValue() {
        AmqpTransferTagGenerator tagGen = new AmqpTransferTagGenerator(false);

        for (int i = 0; i < 512; ++i) {
            byte[] tag = tagGen.getNextTag();

            if (i < 256) {
                assertEquals(1, tag.length);
            } else {
                assertEquals(2, tag.length);
            }
        }
    }

    @Test
    public void testTagValueMatchesParsedArray() throws IOException {
        AmqpTransferTagGenerator tagGen = new AmqpTransferTagGenerator(false);

        for (int i = 0; i < Short.MAX_VALUE; ++i) {
            byte[] tag = tagGen.getNextTag();

            ByteArrayInputStream bais = new ByteArrayInputStream(tag);
            DataInputStream dis = new DataInputStream(bais);

            if (i < 256) {
                assertEquals(1, tag.length);
                assertEquals((byte) i, dis.readByte());
            } else {
                assertEquals(2, tag.length);
                assertEquals(i, dis.readShort());
            }
        }
    }

    @Test
    public void testTagGenerationWorksWithIdRollover() throws Exception {
        AmqpTransferTagGenerator tagGen = new AmqpTransferTagGenerator(false);

        Field urisField = tagGen.getClass().getDeclaredField("nextTagId");
        urisField.setAccessible(true);
        urisField.set(tagGen, Long.MAX_VALUE + 1);

        {
            byte[] tag = tagGen.getNextTag();

            ByteArrayInputStream bais = new ByteArrayInputStream(tag);
            DataInputStream dis = new DataInputStream(bais);

            assertEquals(8, tag.length);
            assertEquals(Long.MAX_VALUE + 1, dis.readLong());
        }
        {
            byte[] tag = tagGen.getNextTag();

            ByteArrayInputStream bais = new ByteArrayInputStream(tag);
            DataInputStream dis = new DataInputStream(bais);

            assertEquals(8, tag.length);
            assertEquals(Long.MAX_VALUE + 2, dis.readLong());
        }
    }

    @Ignore("Used to test performance")
    @Test
    public void testTagGeneratorOverTime() {
        final AmqpTransferTagGenerator tagGen = new AmqpTransferTagGenerator(true);
        final int tagLoop = AmqpTransferTagGenerator.DEFAULT_TAG_POOL_SIZE;
        final ArrayList<byte[]> tags = new ArrayList<>(tagLoop);

        for (int i = 0; i < Short.MAX_VALUE * 16; ++i) {
            // Checkout all the tags the pool will create
            for (int j = 0; j < tagLoop; ++j) {
                tags.add(tagGen.getNextTag());
            }

            // Return them and then clear
            tags.forEach((tag) -> tagGen.returnTag(tag));
            tags.clear();
        }
    }
}
