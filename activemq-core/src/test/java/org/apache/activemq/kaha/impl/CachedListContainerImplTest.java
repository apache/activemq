/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */


package org.apache.activemq.kaha.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import junit.framework.TestCase;

import org.apache.activemq.kaha.IndexTypes;
import org.apache.activemq.kaha.StoreFactory;
import org.apache.activemq.kaha.impl.container.ContainerId;
import org.apache.activemq.kaha.impl.container.ListContainerImpl;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexManager;
/**
 * Junit tests for CachedListContainerImpl
 * 
 * @version $Revision: 439552 $
 */
public class CachedListContainerImplTest extends TestCase{
    protected String name;
    protected KahaStore store;
    protected int MAX_CACHE_SIZE=10;

    protected KahaStore getStore() throws IOException{
        KahaStore store = new KahaStore(name,"rw");
        store.initialize();
        return store;
    }

    public void testAdds() throws Exception{
        ListContainerImpl list=getStoreList("test");
        List data=getDataList(100);
        list.addAll(data);
        assertEquals(MAX_CACHE_SIZE,list.getCacheList().size());
        List cached=getCachedList(MAX_CACHE_SIZE);
        for(int i=0;i<cached.size();i++){
            list.add(i,cached.get(i));
        }
        assertEquals(MAX_CACHE_SIZE,list.getCacheList().size());
        for(int i=0;i<cached.size();i++){
            assertEquals(cached.get(i),list.getCacheList().get(i));
        }
    }

    public void testAddsIntoCacheSpace() throws Exception{
        ListContainerImpl list=getStoreList("test");
        int initialDataSize=50;
        List data=getDataList(initialDataSize);
        list.addAll(data);
        assertEquals(MAX_CACHE_SIZE,list.getCacheList().size());
        List cached=getCachedList(MAX_CACHE_SIZE);
        for(int i=MAX_CACHE_SIZE/2;i<cached.size();i++){
            list.add(i,cached.get(i));
        }
        assertEquals(MAX_CACHE_SIZE,list.getCacheList().size());
        for(int i=0;i<MAX_CACHE_SIZE/2;i++){
            assertEquals(data.get(i),list.getCacheList().get(i));
        }
        for(int i=MAX_CACHE_SIZE/2;i<MAX_CACHE_SIZE;i++){
            assertEquals(cached.get(i),list.getCacheList().get(i));
        }
    }

    public void testRemoves() throws Exception{
        ListContainerImpl list=getStoreList("test");
        int initialDataSize=10;
        List data=getDataList(initialDataSize);
        list.addAll(data);
        assertEquals(MAX_CACHE_SIZE,list.getCacheList().size());
        List cached=getCachedList(MAX_CACHE_SIZE);
        list.addAll(cached);
        assertEquals(MAX_CACHE_SIZE,list.getCacheList().size());
        for(int i=0;i<cached.size();i++){
            assertNotSame(cached.get(i),list.getCacheList().get(i));
        }
        for(int i=0;i<initialDataSize;i++){
            list.remove(0);
        }
        assertEquals(0,list.getCacheList().size());
        // repopulate the cache
        for(int i=0;i<MAX_CACHE_SIZE;i++){
            list.get(i);
        }
        assertEquals(MAX_CACHE_SIZE,list.getCacheList().size());
        for(int i=0;i<cached.size();i++){
            assertEquals(cached.get(i),list.getCacheList().get(i));
        }
    }

    public void testCacheSize() throws Exception{
        ListContainerImpl list=getStoreList("test");
        List data=getDataList(100);
        list.addAll(data);
        assertEquals(MAX_CACHE_SIZE,list.getCacheList().size());
    }

    public void testInserts() throws Exception{
        ListContainerImpl list=getStoreList("test");
        List data=getDataList(100);
        list.addAll(data);
        assertEquals(MAX_CACHE_SIZE,list.getCacheList().size());
        List cached=getCachedList(MAX_CACHE_SIZE);
        for(int i=0;i<cached.size();i++){
            list.set(i,cached.get(i));
        }
        assertEquals(MAX_CACHE_SIZE,list.getCacheList().size());
        for(int i=0;i<cached.size();i++){
            assertEquals(cached.get(i),list.getCacheList().get(i));
        }
    }

    protected ListContainerImpl getStoreList(Object id) throws Exception{
        String containerName="test";
        DataManager dm=store.getDataManager(containerName);
        IndexManager im=store.getIndexManager(dm,containerName);
        ContainerId containerId=new ContainerId();
        containerId.setKey(id);
        containerId.setDataContainerName(containerName);
        IndexItem root=store.getListsContainer().addRoot(im,containerId);
        ListContainerImpl result=new ListContainerImpl(containerId,root,im,dm,IndexTypes.DISK_INDEX);
        result.expressDataInterest();
        result.setMaximumCacheSize(MAX_CACHE_SIZE);
        return result;
    }

    protected List getDataList(int num){
        List result=new ArrayList();
        for(int i=0;i<num;i++){
            result.add("data:"+i);
        }
        return result;
    }

    protected List getCachedList(int num){
        List result=new ArrayList();
        for(int i=0;i<num;i++){
            result.add("cached:"+i);
        }
        return result;
    }

    protected void setUp() throws Exception{
        super.setUp();
        name=System.getProperty("basedir",".")+"/target/activemq-data/store-test.db";
        store=getStore();
    }

    protected void tearDown() throws Exception{
        super.tearDown();
        if(store!=null){
            store.close();
            store=null;
        }
        boolean rc=StoreFactory.delete(name);
        assertTrue(rc);
    }
}
