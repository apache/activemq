/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */


package org.apache.activemq.kaha;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreFactory;
import junit.framework.TestCase;

/**
*Store test
* 
* @version $Revision: 1.2 $
*/
public class StoreTest extends TestCase{
    
    protected String name = "sdbStoreTest.db";
    protected Store store;
    

    /*
     * Test method for 'org.apache.activemq.kaha.Store.close()'
     */
    public void testClose() throws Exception{
        store.close();
        try {
            //access should throw an exception
            store.getListContainer("fred");
            assertTrue("Should have got a enception",false);
        }catch(Exception e){
            
        }
    }

    /*
     * Test method for 'org.apache.activemq.kaha.Store.clear()'
     */
    public void testClear() throws Exception{
        int count = 100;
        ListContainer list = store.getListContainer("testClear");
        list.load();
        for (int i =0; i < count; i++){
            list.add("test " + i);
        }
        assertEquals(count,list.size());
        store.clear();
        assertTrue(list.isEmpty());
    }

   

    /*
     * Test method for 'org.apache.activemq.kaha.Store.getMapContainer(Object)'
     */
    public void testGetMapContainer() throws Exception{
        String containerId = "test";
        MapContainer container = store.getMapContainer(containerId);
        container.load();
        assertNotNull(container);
        store.close();
        store = getStore();
        container = store.getMapContainer(containerId);
        assertNotNull(container);
        
        
        
    }

    /*
     * Test method for 'org.apache.activemq.kaha.Store.deleteMapContainer(Object)'
     */
    public void testDeleteMapContainer() throws Exception{
        String containerId = "test";
        MapContainer container = store.getMapContainer(containerId);
        assertNotNull(container);
        store.deleteMapContainer(containerId);
        assertFalse(store.doesMapContainerExist(containerId));
        store.close();
        store = getStore();
        assertFalse(store.doesMapContainerExist(containerId));
    }

    /*
     * Test method for 'org.apache.activemq.kaha.Store.getMapContainerIds()'
     */
    public void testGetMapContainerIds()throws Exception {
        String containerId = "test";
        MapContainer container = store.getMapContainer(containerId);
        Set set = store.getMapContainerIds();
        assertTrue(set.contains(containerId));
    }

    

    /*
     * Test method for 'org.apache.activemq.kaha.Store.getListContainer(Object)'
     */
    public void testGetListContainer() throws Exception{
        String containerId = "test";
        ListContainer container = store.getListContainer(containerId);
        assertNotNull(container);
        store.close();
        store = getStore();
        container = store.getListContainer(containerId);
        assertNotNull(container);
    }

    /*
     * Test method for 'org.apache.activemq.kaha.Store.deleteListContainer(Object)'
     */
    public void testDeleteListContainer()throws Exception{
        String containerId = "test";
        ListContainer container = store.getListContainer(containerId);
        assertNotNull(container);
        store.deleteListContainer(containerId);
        assertFalse(store.doesListContainerExist(containerId));
        store.close();
        store = getStore();
        assertFalse(store.doesListContainerExist(containerId));
    }

    /*
     * Test method for 'org.apache.activemq.kaha.Store.getListContainerIds()'
     */
    public void testGetListContainerIds()throws Exception {
        String containerId = "test";
        ListContainer container = store.getListContainer(containerId);
        Set set = store.getListContainerIds();
        assertTrue(set.contains(containerId));
    }
    
    public void testBasicAllocations() throws Exception{
        Map testMap = new HashMap();
        for (int i =0; i<10; i++){
            String key = "key:"+i;
            String value = "value:"+i;
            testMap.put(key, value);
        }
        List testList = new ArrayList();
        for (int i = 0; i < 10; i++){
            testList.add("value:"+i);
        }
        String listId = "testList";
        String mapId = "testMap";
        MapContainer mapContainer = store.getMapContainer(mapId);
        mapContainer.load();
        ListContainer listContainer = store.getListContainer(listId);
        listContainer.load();
        mapContainer.putAll(testMap);
        listContainer.addAll(testList);
        store.close();
        store = getStore();
        mapContainer = store.getMapContainer(mapId);
        mapContainer.load();
        listContainer = store.getListContainer(listId);
        listContainer.load();
        for (Iterator i = testMap.keySet().iterator(); i.hasNext();){
            Object key = i.next();
            Object value = testMap.get(key);
            assertTrue(mapContainer.containsKey(key));
            assertEquals(value,mapContainer.get(key));
        }
        assertEquals(testList.size(),listContainer.size());
        for (Iterator i = testList.iterator(), j = listContainer.iterator(); i.hasNext();){
            assertEquals(i.next(),j.next());
        }
    }
    
    
    protected Store getStore() throws IOException{
        return StoreFactory.open(name, "rw");
    }
    
    protected void setUp() throws Exception{
        super.setUp();
        store = getStore();
        
    }

    protected void tearDown() throws Exception{
        super.tearDown();
        if( store!=null ) {
        	store.close();
        	store=null;
        }
        boolean rc = StoreFactory.delete(name);
        assertTrue(rc);
    }
}
