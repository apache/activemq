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
package org.apache.activemq.kaha.impl.index;

import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import org.apache.activemq.kaha.StoreEntry;

/**
 * @version $Revision: 1.2 $
 */
public class VMIndexLinkedListTest extends TestCase {
    static final int NUMBER = 10;
    private IndexItem root;
    private List testData = new ArrayList();
    private IndexLinkedList list;

    protected void setUp() throws Exception {
        super.setUp();
        for (int i = 0; i < NUMBER; i++) {
            IndexItem item = new IndexItem();
            item.setOffset(i);
            testData.add(item);
        }
        root = new IndexItem();
        list = new VMIndexLinkedList(root);
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        testData.clear();
        list = null;
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.getFirst()'
     */
    public void testGetFirst() {
        for (int i = 0; i < testData.size(); i++) {
            list.add((IndexItem)testData.get(i));
        }
        assertTrue(list.getFirst() == testData.get(0));
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.getLast()'
     */
    public void testGetLast() {
        for (int i = 0; i < testData.size(); i++) {
            list.add((IndexItem)testData.get(i));
        }
        assertTrue(list.getLast() == testData.get(testData.size() - 1));
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.removeFirst()'
     */
    public void testRemoveFirst() {
        for (int i = 0; i < testData.size(); i++) {
            list.add((IndexItem)testData.get(i));
        }
        assertTrue(list.removeFirst() == testData.get(0));
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.removeLast()'
     */
    public void testRemoveLast() {
        for (int i = 0; i < testData.size(); i++) {
            list.add((IndexItem)testData.get(i));
        }
        assertTrue(list.removeLast() == testData.get(testData.size() - 1));
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.addFirst(IndexItem)'
     */
    public void testAddFirst() {
        for (int i = 0; i < testData.size(); i++) {
            list.addFirst((IndexItem)testData.get(i));
        }
        int count = 0;
        for (int i = testData.size() - 1; i >= 0; i--) {
            assertTrue(testData.get(i) == list.get(count++));
        }
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.addLast(IndexItem)'
     */
    public void testAddLast() {
        for (int i = 0; i < testData.size(); i++) {
            list.addLast((IndexItem)testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            assertTrue(testData.get(i) == list.get(i));
        }
    }

    /*
     * Test method for 'org.apache.activemq.kaha.impl.VMIndexLinkedList.size()'
     */
    public void testSize() {
        for (int i = 0; i < testData.size(); i++) {
            list.addLast((IndexItem)testData.get(i));
            assertTrue(list.size() == i + 1);
        }
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.isEmpty()'
     */
    public void testIsEmpty() {
        for (int i = 0; i < testData.size(); i++) {
            list.addLast((IndexItem)testData.get(i));
            assertTrue(list.size() == i + 1);
        }
        list.clear();
        assertTrue(list.isEmpty());
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.add(IndexItem)'
     */
    public void testAddIndexItem() {
        for (int i = 0; i < testData.size(); i++) {
            list.add((IndexItem)testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            assertTrue(testData.get(i) == list.get(i));
        }
    }

    /*
     * Test method for 'org.apache.activemq.kaha.impl.VMIndexLinkedList.clear()'
     */
    public void testClear() {
        for (int i = 0; i < testData.size(); i++) {
            list.addLast((IndexItem)testData.get(i));
            assertTrue(list.size() == i + 1);
        }
        list.clear();
        assertTrue(list.isEmpty());
    }

    /*
     * Test method for 'org.apache.activemq.kaha.impl.VMIndexLinkedList.add(int,
     * IndexItem)'
     */
    public void testAddIntIndexItem() {
        for (int i = 0; i < testData.size(); i++) {
            list.add(i, (IndexItem)testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            assertTrue(testData.get(i) == list.get(i));
        }
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.remove(int)'
     */
    public void testRemoveInt() {
        for (int i = 0; i < testData.size(); i++) {
            list.add(i, (IndexItem)testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            list.remove(0);
        }
        assertTrue(list.isEmpty());
        for (int i = 0; i < testData.size(); i++) {
            list.add(i, (IndexItem)testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            list.remove(list.size() - 1);
        }
        assertTrue(list.isEmpty());
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.indexOf(IndexItem)'
     */
    public void testIndexOf() {
        for (int i = 0; i < testData.size(); i++) {
            list.add(i, (IndexItem)testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            assertTrue(list.indexOf((StoreEntry)testData.get(i)) == i);
        }
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.getNextEntry(IndexItem)'
     */
    public void testGetNextEntry() {
        for (int i = 0; i < testData.size(); i++) {
            list.add(i, (IndexItem)testData.get(i));
        }
        IndexItem next = list.getFirst();
        int count = 0;
        while (next != null) {
            assertTrue(next == testData.get(count++));
            next = list.getNextEntry(next);
            assertTrue(next != root);
        }
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.getPrevEntry(IndexItem)'
     */
    public void testGetPrevEntry() {
        for (int i = 0; i < testData.size(); i++) {
            list.add(i, (IndexItem)testData.get(i));
        }
        IndexItem next = list.getLast();
        int count = testData.size() - 1;
        while (next != null) {
            assertTrue(next == testData.get(count--));
            next = list.getPrevEntry(next);
            assertTrue(next != root);
        }
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.remove(IndexItem)'
     */
    public void testRemoveIndexItem() {
        for (int i = 0; i < testData.size(); i++) {
            list.add(i, (IndexItem)testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            list.remove((IndexItem)testData.get(i));
            assertTrue(list.size() == testData.size() - i - 1);
        }
    }
}
