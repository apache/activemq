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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.impl.data.Item;

/**
 * 
 */
public class VMIndexLinkedListTest extends TestCase {
    static final int NUMBER = 30;
    private IndexItem root;
    private List<IndexItem> testData = new ArrayList<IndexItem>();
    private IndexLinkedList list;

    protected void setUp() throws Exception {
        super.setUp();
        
        IndexItem item = new IndexItem();
        list = createList(item);
        this.root = list.getRoot();
        
        for (int i = 0; i < NUMBER; i++) {
            item = createIndex(list,i);
            testData.add(item);
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        testData.clear();
        list = null;
    }
    
    IndexItem createIndex(IndexLinkedList list,int offset) throws IOException {
        IndexItem result =  new IndexItem();
        result.setOffset(offset);
        return result;
    }
    protected IndexLinkedList createList(IndexItem root) throws IOException {
        return new VMIndexLinkedList(root);
    }
    
    protected void addToList(IndexLinkedList list,IndexItem item) throws IOException {
        list.add(item);
    }
    
    protected void insertToList(IndexLinkedList list,int pos,IndexItem item) throws IOException {
        list.add(pos, item);
    }
    
    protected void insertFirst(IndexLinkedList list,IndexItem item) throws IOException {
        list.addFirst(item);
    }
    
    protected synchronized void remove(IndexLinkedList list,IndexItem item) throws IOException {
        IndexItem root = list.getRoot();
        IndexItem prev = list.getPrevEntry(item);
        IndexItem next = list.getNextEntry(item);
        list.remove(item);
    }
    

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.getFirst()'
     */
    public void testGetFirst() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
        }
        assertNotNull(list.getFirst());
        assertTrue(list.getFirst().equals(testData.get(0)));
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.getLast()'
     */
    public void testGetLast() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
        }
        assertTrue(list.getLast() == testData.get(testData.size() - 1));
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.removeFirst()'
     */
    public void testRemoveFirst() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
        }
        assertTrue(list.removeFirst().equals(testData.get(0)));
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.removeLast()'
     */
    public void testRemoveLast() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
        }
        assertTrue(list.removeLast().equals(testData.get(testData.size() - 1)));
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.addFirst(IndexItem)'
     */
    public void testAddFirst() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            insertFirst(list, testData.get(i));
        }
        int count = 0;
        for (int i = testData.size() - 1; i >= 0; i--) {
            assertTrue(testData.get(i).equals(list.get(count++)));
        }
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.addLast(IndexItem)'
     */
    public void testAddLast() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            assertTrue(testData.get(i).equals(list.get(i)));
        }
    }

    /*
     * test method for 'org.apache.activemq.kaha.impl.VMIndexLinkedList.size()'
     */
    public void testSize() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
            assertTrue(list.size() == i + 1);
        }
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.isEmpty()'
     */
    public void testIsEmpty() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
            assertTrue(list.size() == i + 1);
        }
        list.clear();
        assertTrue(list.isEmpty());
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.add(IndexItem)'
     */
    public void testAddIndexItem() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            assertTrue(testData.get(i).equals(list.get(i)));
        }
    }

    /*
     * test method for 'org.apache.activemq.kaha.impl.VMIndexLinkedList.clear()'
     */
    public void testClear() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
            assertTrue(list.size() == i + 1);
        }
        list.clear();
        assertTrue(list.isEmpty());
    }

    /*
     * test method for 'org.apache.activemq.kaha.impl.VMIndexLinkedList.add(int,
     * IndexItem)'
     */
    public void testAddIntIndexItem() throws IOException {
        for (int i = 0; i < this.testData.size(); i++) {
            insertToList(list, i, testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            assertTrue(testData.get(i).equals(list.get(i)));
        }
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.remove(int)'
     */
    public void testRemoveInt() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            insertToList(list, i, testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            list.remove(0);
        }
        assertTrue(list.isEmpty());
        for (int i = 0; i < testData.size(); i++) {
            insertToList(list, i, testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            list.remove(list.size() - 1);
        }
        assertTrue(list.isEmpty());
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.indexOf(IndexItem)'
     */
    public void testIndexOf() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            assertTrue(list.indexOf(testData.get(i)) == i);
        }
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.getNextEntry(IndexItem)'
     */
    public void testGetNextEntry() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
        }
        IndexItem next = list.getFirst();
        int count = 0;
        while (next != null) {
            assertTrue(next.equals(testData.get(count++)));
            next = list.getNextEntry(next);
            assertTrue(next == null || !next.equals(root));
        }
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.getPrevEntry(IndexItem)'
     */
    public void testGetPrevEntry() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
        }
        IndexItem next = list.getLast();
        int count = testData.size() - 1;
        while (next != null) {
            assertTrue(next.equals(testData.get(count--)));
            next = list.getPrevEntry(next);
            assertTrue(next == null || !next.equals(root));
        }
    }

    /*
     * test method for
     * 'org.apache.activemq.kaha.impl.VMIndexLinkedList.remove(IndexItem)'
     */
    public void testRemoveIndexItem() throws IOException {
        for (int i = 0; i < testData.size(); i++) {
            addToList(list,testData.get(i));
        }
        for (int i = 0; i < testData.size(); i++) {
            list.remove(testData.get(i));
            assertTrue(list.size() == testData.size() - i - 1);
        }
    }
    
    public void testAddRemove() throws IOException {
        IndexItem a = createIndex(list,0);
        addToList(list, a);
        IndexItem b = createIndex(list,1);
        addToList(list, b);
        IndexItem c = createIndex(list,2);
        addToList(list, c);
        IndexItem d = createIndex(list,3);
        addToList(list, d);
        remove(list, d);
        assertTrue(list.getLast().equals(c));
        assertTrue(list.getNextEntry(b).equals(c));
        remove(list, b);
        assertTrue(list.getNextEntry(a).equals(c));
        assertTrue(list.getLast().equals(c));
        
    }
}
