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
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.activemq.kaha.impl.container.BaseContainerImpl;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import junit.framework.TestCase;

public class MapContainerTest extends TestCase {
    private static final Log LOG = LogFactory.getLog(MapContainerTest.class);
    protected static final int COUNT = 10;

    protected String name = "test";
    protected Store store;
    protected MapContainer<String, String> container;
    protected Map<String, String> testMap;

    public void testBasicAllocations() throws Exception {
        String key = "key";
        Object value = testMap;
        MapContainer<String, Object> test = store.getMapContainer("test", "test");
        test.put(key, value);
        store.close();
        store = getStore();
        assertFalse(store.getMapContainerIds().isEmpty());
        test = store.getMapContainer("test", "test");
        assertEquals(value, test.get(key));

    }

    /*
     * Test method for 'org.apache.activemq.kaha.MapContainer.size()'
     */
    public void testSize() throws Exception {
        container.putAll(testMap);
        assertTrue(container.size() == testMap.size());
    }

    /*
     * Test method for 'org.apache.activemq.kaha.MapContainer.isEmpty()'
     */
    public void testIsEmpty() throws Exception {
        assertTrue(container.isEmpty());
    }

    /*
     * Test method for 'org.apache.activemq.kaha.MapContainer.clear()'
     */
    public void testClear() throws Exception {
        container.putAll(testMap);
        assertTrue(container.size() == testMap.size());
        container.clear();
        assertTrue(container.isEmpty());
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.MapContainer.containsKey(Object)'
     */
    public void testContainsKeyObject() throws Exception {
        container.putAll(testMap);
        for (Iterator i = testMap.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Entry)i.next();
            assertTrue(container.containsKey(entry.getKey()));
        }
    }

    /*
     * Test method for 'org.apache.activemq.kaha.MapContainer.get(Object)'
     */
    public void testGetObject() throws Exception {
        container.putAll(testMap);
        for (Iterator i = testMap.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Entry)i.next();
            Object value = container.get(entry.getKey());
            assertNotNull(value);
            assertTrue(value.equals(entry.getValue()));
        }
    }

    /*
     * Test method for
     * 'org.apache.activemq.kaha.MapContainer.containsValue(Object)'
     */
    public void testContainsValueObject() throws Exception {
        container.putAll(testMap);
        for (Iterator i = testMap.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Entry)i.next();
            assertTrue(container.containsValue(entry.getValue()));
        }
    }

    /*
     * Test method for 'org.apache.activemq.kaha.MapContainer.putAll(Map)'
     */
    public void testPutAllMap() throws Exception {
        container.putAll(testMap);
        for (Iterator i = testMap.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry = (Entry)i.next();
            assertTrue(container.containsValue(entry.getValue()));
            assertTrue(container.containsKey(entry.getKey()));
        }
    }

    /*
     * Test method for 'org.apache.activemq.kaha.MapContainer.keySet()'
     */
    public void testKeySet() throws Exception {
        container.putAll(testMap);
        Set<String> keys = container.keySet();
        assertTrue(keys.size() == testMap.size());
        for (Iterator<String> i = testMap.keySet().iterator(); i.hasNext();) {
            Object key = i.next();
            assertTrue(keys.contains(key));
            keys.remove(key);
        }
        assertTrue(container.isEmpty());

    }

    /*
     * Test method for 'org.apache.activemq.kaha.MapContainer.values()'
     */
    public void testValues() throws Exception {
        container.putAll(testMap);
        Collection<String> values = container.values();
        assertTrue(values.size() == testMap.size());
        for (Iterator<String> i = testMap.values().iterator(); i.hasNext();) {
            Object value = i.next();
            assertTrue(values.contains(value));
            assertTrue(values.remove(value));
        }
        assertTrue(container.isEmpty());
    }

    /*
     * Test method for 'org.apache.activemq.kaha.MapContainer.entrySet()'
     */
    public void testEntrySet() throws Exception {
        container.putAll(testMap);
        Set entries = container.entrySet();
        assertTrue(entries.size() == testMap.size());
        for (Iterator i = entries.iterator(); i.hasNext();) {
            Map.Entry entry = (Entry)i.next();
            assertTrue(testMap.containsKey(entry.getKey()));
            assertTrue(testMap.containsValue(entry.getValue()));

        }

    }

    /*
     * Test method for 'org.apache.activemq.kaha.MapContainer.remove(Object)'
     */
    public void testRemoveObject() throws Exception {
        container.putAll(testMap);
        for (Iterator<String> i = testMap.keySet().iterator(); i.hasNext();) {
            container.remove(i.next());
        }
        assertTrue(container.isEmpty());
    }

    
    public void testDuplicatesOk() throws Exception {
        StoreEntry first, entry; 
        
        container.put("M1", "DD");
        first = container.getFirst();
        LOG.info("First=" + first);
        assertEquals(-1, first.getNextItem());
        
        // add duplicate
        String old = container.put("M1", "DD");
        assertNotNull(old);
        assertEquals(1, container.size());
        
        entry = container.getFirst();
        LOG.info("New First=" + entry);
        assertEquals(-1, entry.getNextItem());

        assertEquals(first, entry);
        
        container.remove("M1");
        
        entry = container.getFirst();
        assertNull(entry);
    }

    
    public void testDuplicatesFreeListShared() throws Exception {
        StoreEntry batchEntry; 
        
        MapContainer other = store.getMapContainer(getName()+"2", "test", true);
        other.load();
        other.put("M1", "DD");
             
        container.put("M1", "DD");
        batchEntry = container.getFirst();
        LOG.info("First=" + batchEntry);
        assertEquals(-1, batchEntry.getNextItem());
        
        // have something on free list before duplicate
        other.remove("M1");
        
        // add duplicate
        String old = container.put("M1", "DD");
        assertNotNull(old);
        assertEquals(1, container.size());

        // entry now on free list on its own
        batchEntry = container.refresh(batchEntry);
        assertEquals(-1, batchEntry.getNextItem());
        LOG.info("refreshed=" + batchEntry);
        
        // ack
        container.remove("M1");   
        
        //container is valid  (empty)
        assertNull(container.getFirst());

        // batchEntry now has next as there is another on the free list
        batchEntry = container.refresh(batchEntry);
        LOG.info("refreshed=" + batchEntry);
        
        assertTrue(batchEntry.getNextItem() != -1);        
    }

    protected Store getStore() throws IOException {
        return StoreFactory.open(name, "rw");
    }

    protected void setUp() throws Exception {
        super.setUp();
        name = System.getProperty("basedir", ".") + "/target/activemq-data/map-container.db";
        store = getStore();
        container = store.getMapContainer(getName(), "test", true);
        container.load();
        testMap = new HashMap<String, String>();
        for (int i = 0; i < COUNT; i++) {
            String key = "key:" + i;
            String value = "value:" + i;
            testMap.put(key, value);
        }

    }

    protected void tearDown() throws Exception {
        super.tearDown();
        if (store != null) {
            store.close();
            store = null;
        }
        assertTrue(StoreFactory.delete(name));
    }

}
