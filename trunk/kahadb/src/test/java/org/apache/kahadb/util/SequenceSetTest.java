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

package org.apache.kahadb.util;

import static org.junit.Assert.*;

import java.util.Iterator;

import org.junit.Test;

public class SequenceSetTest {

    @Test
    public void testAddLong() {
        SequenceSet set = new SequenceSet();
        set.add(1);
        assertEquals(1, set.rangeSize());
        set.add(10);
        set.add(20);
        assertEquals(3, set.rangeSize());
    }

    @Test
    public void testRangeSize() {
        SequenceSet set = new SequenceSet();
        set.add(1);
        assertEquals(1, set.rangeSize());
        set.add(10);
        set.add(20);
        assertEquals(3, set.rangeSize());
        set.clear();
        assertEquals(0, set.rangeSize());
    }

    @Test
    public void testIsEmpty() {
        SequenceSet set = new SequenceSet();
        assertTrue(set.isEmpty());
        set.add(1);
        assertFalse(set.isEmpty());
    }

    @Test
    public void testClear() {
        SequenceSet set = new SequenceSet();
        set.clear();
        assertTrue(set.isEmpty());
        set.add(1);
        assertFalse(set.isEmpty());
        set.clear();
        assertTrue(set.isEmpty());
    }

    @Test
    public void testContains() {
        SequenceSet set = new SequenceSet();
        set.add(new Sequence(0, 10));
        set.add(new Sequence(21, 42));
        set.add(new Sequence(47, 90));
        set.add(new Sequence(142, 512));

        assertTrue(set.contains(0));
        assertTrue(set.contains(42));
        assertTrue(set.contains(49));
        assertTrue(set.contains(153));

        assertFalse(set.contains(43));
        assertFalse(set.contains(99));
        assertFalse(set.contains(-1));
        assertFalse(set.contains(11));
    }

    @Test
    public void testRemove() {
        SequenceSet set = new SequenceSet();
        set.add(new Sequence(0, 100));
        assertEquals(101, set.rangeSize());

        assertEquals(1, set.size());
        assertFalse(set.remove(101));
        assertTrue(set.remove(50));
        assertEquals(2, set.size());
        assertEquals(100, set.rangeSize());
        assertFalse(set.remove(101));

        set.remove(0);
        assertEquals(2, set.size());
        assertEquals(99, set.rangeSize());
        set.remove(100);
        assertEquals(2, set.size());
        assertEquals(98, set.rangeSize());

        set.remove(10);
        assertEquals(3, set.size());
        assertEquals(97, set.rangeSize());
    }

    @Test
    public void testIterator() {
        SequenceSet set = new SequenceSet();
        set.add(new Sequence(0, 2));
        set.add(new Sequence(4, 5));
        set.add(new Sequence(7));
        set.add(new Sequence(20, 21));

        long expected[] = new long[]{0, 1, 2, 4, 5, 7, 20, 21};
        int index = 0;

        Iterator<Long> iterator = set.iterator();
        while(iterator.hasNext()) {
            assertEquals(expected[index++], iterator.next().longValue());
        }
    }

    @Test
    public void testIteratorEmptySequenceSet() {
        SequenceSet set = new SequenceSet();

        Iterator<Long> iterator = set.iterator();
        while(iterator.hasNext()) {
            fail("Should not have any elements");
        }
    }
}
