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
package org.apache.activemq.kaha;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import junit.framework.TestCase;

public class ListContainerTest extends TestCase {
    
    protected static final int COUNT = 10;

    protected String name = "test";
    protected Store store;
    protected ListContainer<Object> container;
    protected LinkedList<Object> testList;

    /*
     * Test method for 'org.apache.activemq.kaha.ListContainer.size()'
     */
    public void testSize() throws Exception {
        container.addAll(testList);
        assertEquals(container.size(), testList.size());
    }

    /*
     * Test method for 'org.apache.activemq.kaha.ListContainer.addFirst(Object)'
     */
    public void testAddFirst() throws Exception {
        container.addAll(testList);
        assertEquals(container.size(), testList.size());
        String first = "first";
        container.addFirst(first);
        assertEquals(first, container.get(0));
        assertEquals(container.size(), testList.size() + 1);
    }

    /*
     * Test method for 'org.apache.activemq.kaha.ListContainer.addLast(Object)'
     */
    public void testAddLast() throws Exception {
        container.addAll(testList);
        assertEquals(container.size(), testList.size());
        String last = "last";
        container.addLast(last);
        assertEquals(last, container.get(testList.size()));
        assertEquals(container.size(), testList.size() + 1);
    }

    /*
     * Test method for 'org.apache.activemq.kaha.ListContainer.removeFirst()'
     */
    public void testRemoveFirst() throws Exception {
        container.addAll(testList);
        assertEquals(container.size(), testList.size());
        assertEquals(testList.get(0), container.removeFirst());
        assertEquals(container.size(), testList.size() - 1);
        for (int i = 1; i < testList.size(); i++) {
            assertEquals(testList.get(i), container.get(i - 1));
        }
    }

    /*
     * Test method for 'org.apache.activemq.kaha.ListContainer.removeLast()'
     */
    public void testRemoveLast() throws Exception {
        container.addAll(testList);
        assertEquals(container.size(), testList.size());
        assertEquals(testList.get(testList.size() - 1), container.removeLast());
        assertEquals(container.size(), testList.size() - 1);
        for (int i = 0; i < testList.size() - 1; i++) {
            assertEquals(testList.get(i), container.get(i));
        }
    }

    /*
     * Test method for 'java.util.List.iterator()'
     */
    public void testIterator() throws Exception {
        container.addAll(testList);
        Iterator<Object> j = container.iterator();
        for (Iterator<Object> i = testList.iterator(); i.hasNext();) {
            assertEquals(i.next(), j.next());
        }
        for (Iterator<Object> i = container.iterator(); i.hasNext();) {
            i.next();
            i.remove();
        }
        assert container.isEmpty();
    }

    /*
     * Test method for 'java.util.List.isEmpty()'
     */
    public void testIsEmpty() throws Exception {
        assertTrue(container.isEmpty());
    }

    /*
     * Test method for 'java.util.List.contains(Object)'
     */
    public void testContains() throws Exception {
        container.addAll(testList);
        for (Iterator<Object> i = testList.iterator(); i.hasNext();) {
            assertTrue(container.contains(i.next()));
        }
    }

    /*
     * Test method for 'java.util.List.toArray()'
     */
    public void testToArray() throws Exception {
        container.addAll(testList);
        Object[] a = testList.toArray();
        Object[] b = container.toArray();
        assertEquals(a.length, b.length);
        for (int i = 0; i < a.length; i++) {
            assertEquals(a[i], b[i]);
        }
    }

    /*
     * Test method for 'java.util.List.remove(Object)'
     */
    public void testRemoveObject() throws Exception {
        container.addAll(testList);
        assertEquals(container.size(), testList.size());
        for (int i = 0; i < testList.size(); i++) {
            container.remove(testList.get(i));
        }
        assertTrue(container.isEmpty());
    }

    /*
     * Test method for 'java.util.List.containsAll(Collection<?>)'
     */
    public void testContainsAll() throws Exception {
        container.addAll(testList);
        assertTrue(container.containsAll(testList));
    }

    /*
     * Test method for 'java.util.List.removeAll(Collection<?>)'
     */
    public void testRemoveAll() throws Exception {
        container.addAll(testList);
        assertEquals(testList.size(), container.size());
        container.removeAll(testList);
        assertTrue(container.isEmpty());
    }

    /*
     * Test method for 'java.util.List.retainAll(Collection<?>)'
     */
    public void testRetainAll() throws Exception {
        container.addAll(testList);
        assertEquals(testList.size(), container.size());
        testList.remove(0);
        container.retainAll(testList);
        assertEquals(testList.size(), container.size());
    }

    /*
     * Test method for 'java.util.List.clear()'
     */
    public void testClear() throws Exception {
        container.addAll(testList);
        assertEquals(testList.size(), container.size());
        container.clear();
        assertTrue(container.isEmpty());
    }

    /*
     * Test method for 'java.util.List.get(int)'
     */
    public void testGet() throws Exception {
        container.addAll(testList);
        for (int i = 0; i < testList.size(); i++) {
            assertEquals(container.get(i), testList.get(i));
        }
    }

    /*
     * Test method for 'java.util.List.set(int, E)'
     */
    public void testSet() throws Exception {
        container.addAll(testList);
    }

    /*
     * Test method for 'java.util.List.add(int, E)'
     */
    public void testAddIntE() throws Exception {
        container.addAll(testList);
        assertTrue(container.equals(testList));
        Object testObj = "testObj";
        int index = 0;
        testList.set(index, testObj);
        container.set(index, testObj);
        assertTrue(container.equals(testList));
        index = testList.size() - 1;
        testList.set(index, testObj);
        container.set(index, testObj);
        assertTrue(container.equals(testList));
    }

    /*
     * Test method for 'java.util.List.remove(int)'
     */
    public void testRemoveInt() throws Exception {
        container.addAll(testList);
        assertTrue(container.equals(testList));
        testList.remove(0);
        container.remove(0);
        assertTrue(container.equals(testList));
        int pos = testList.size() - 1;
        testList.remove(pos);
        container.remove(pos);
        assertTrue(container.equals(testList));
    }

    /*
     * Test method for 'java.util.List.indexOf(Object)'
     */
    public void testIndexOf() throws Exception {
        container.addAll(testList);
        assertTrue(container.equals(testList));
        for (int i = 0; i < testList.size(); i++) {
            Object o = testList.get(i);
            assertEquals(i, container.indexOf(o));
        }
    }

    /*
     * Test method for 'java.util.List.listIterator()'
     */
    public void testListIterator() throws Exception {
        container.addAll(testList);
        ListIterator<Object> containerIter = container.listIterator();
        ListIterator<Object> testIter = testList.listIterator();
        assertTrue(testIter.hasNext());
        assertTrue(containerIter.hasNext());
        while (testIter.hasNext()) {
            Object o1 = testIter.next();
            Object o2 = containerIter.next();
            assertEquals(o1, o2);
            testIter.remove();
            containerIter.remove();
        }
        assertTrue(testList.isEmpty());
        assertTrue(container.isEmpty());
    }

    /*
     * Test method for 'java.util.List.listIterator(int)'
     */
    public void testListIteratorInt() throws Exception {
        container.addAll(testList);
        int start = testList.size() / 2;
        ListIterator<Object> containerIter = container.listIterator(start);
        ListIterator<Object> testIter = testList.listIterator(start);
        assertTrue(testIter.hasNext());
        assertTrue(containerIter.hasNext());
        while (testIter.hasNext()) {
            Object o1 = testIter.next();
            Object o2 = containerIter.next();
            assertEquals(o1, o2);
        }
    }

    /*
     * Test method for 'java.util.List.subList(int, int)'
     */
    public void testSubList() throws Exception {
        container.addAll(testList);
        int start = testList.size() / 2;
        List<Object> l1 = testList.subList(start, testList.size());
        List<Object> l2 = container.subList(start, testList.size());
        assertEquals(l1.size(), l2.size());
        assertEquals(l1, l2);
    }

    protected Store getStore() throws IOException {
        return StoreFactory.open(name, "rw");
    }

    protected void setUp() throws Exception {
        super.setUp();
        name = System.getProperty("basedir", ".") + "/target/activemq-data/list-container.db";
        StoreFactory.delete(name);
        store = getStore();
        store.deleteListContainer(name);
        container = store.getListContainer(name);
        container.load();
        testList = new LinkedList<Object>();
        for (int i = 0; i < COUNT; i++) {
            String value = "value:" + i;
            testList.add(value);
        }
    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (store != null) {
            store.close();
        }
        assertTrue(StoreFactory.delete(name));
    }
}
