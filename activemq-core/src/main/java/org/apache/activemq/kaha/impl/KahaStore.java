/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.kaha.impl;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.activemq.kaha.Store;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
/**
 * Optimized Store writer
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class KahaStore implements Store{
    private static final String DEFAULT_CONTAINER_NAME = "data-container";
    private File directory;
    private DataManager rootData;
    private DataManager defaultContainerManager;
    private IndexManager indexManager;
    private IndexRootContainer mapsContainer;
    private IndexRootContainer listsContainer;
    private Map lists=new ConcurrentHashMap();
    private Map maps=new ConcurrentHashMap();
    private Map dataManagers = new ConcurrentHashMap();
    private boolean closed=false;
    private String name;
    private String mode;
    private boolean initialized;

    public KahaStore(String name,String mode) throws IOException{
        this.name=name;
        this.mode=mode;
        initialize();
    }

    public synchronized void close() throws IOException{
        if(!closed){
            closed=true;
            if(initialized){
                indexManager.close();
                rootData.close();
                defaultContainerManager.close();
            }
        }
    }

    public synchronized void force() throws IOException{
        if(initialized){
            indexManager.force();
            rootData.force();
            defaultContainerManager.force();
        }
    }

    public synchronized void clear() throws IOException{
        initialize();
        for(Iterator i=maps.values().iterator();i.hasNext();){
            BaseContainerImpl container=(BaseContainerImpl) i.next();
            container.clear();
        }
        for(Iterator i=lists.values().iterator();i.hasNext();){
            BaseContainerImpl container=(BaseContainerImpl) i.next();
            container.clear();
        }
        lists.clear();
        maps.clear();
    }

    public synchronized boolean delete() throws IOException{
        initialize();
        clear();
        boolean result=indexManager.delete();
        result&=rootData.delete();
        result&=defaultContainerManager.delete();
        initialized=false;
        return result;
    }

    public boolean doesMapContainerExist(Object id) throws IOException{
        initialize();
        return maps.containsKey(id);
    }

    public MapContainer getMapContainer(Object id) throws IOException{
        return getMapContainer(id, DEFAULT_CONTAINER_NAME);
    }
    
    public synchronized MapContainer getMapContainer(Object id, String dataContainerName) throws IOException{
        initialize();
       
        MapContainer result=(MapContainer) maps.get(id);
        if(result==null){
            DataManager dm = getDataManager(dataContainerName);
            ContainerId containerId = new ContainerId();
            containerId.setKey(id);
            containerId.setDataContainerPrefix(dataContainerName);
            IndexItem root=mapsContainer.addRoot(containerId);
            result=new MapContainerImpl(containerId,root,indexManager,dm);
            maps.put(containerId.getKey(),result);
        }
        return result;
    }

    public void deleteMapContainer(Object id) throws IOException{
        initialize();
        MapContainerImpl container=(MapContainerImpl) maps.remove(id);
        if(container!=null){
            container.clear();
            mapsContainer.removeRoot(container.getContainerId());
        }
    }

    public Set getMapContainerIds() throws IOException{
        initialize();
        return maps.keySet();
    }

    public boolean doesListContainerExist(Object id) throws IOException{
        initialize();
        return lists.containsKey(id);
    }

    public ListContainer getListContainer(Object id) throws IOException{
        return getListContainer(id,DEFAULT_CONTAINER_NAME);
    }
    
    public synchronized ListContainer getListContainer(Object id, String dataContainerName) throws IOException{
        initialize();
       
        ListContainer result=(ListContainer) lists.get(id);
        if(result==null){
            DataManager dm = getDataManager(dataContainerName);
            ContainerId containerId = new ContainerId();
            containerId.setKey(id);
            containerId.setDataContainerPrefix(dataContainerName);
            IndexItem root=listsContainer.addRoot(containerId);
            result=new ListContainerImpl(containerId,root,indexManager,dm);
            lists.put(containerId.getKey(),result);
        }
        return result;
    }

    public void deleteListContainer(Object id) throws IOException{
        initialize();
        ListContainerImpl container=(ListContainerImpl) lists.remove(id);
        if(container!=null){
            container.clear();
            listsContainer.removeRoot(container.getContainerId());
        }
    }

    public Set getListContainerIds() throws IOException{
        initialize();
        return lists.keySet();
    }

    protected void checkClosed(){
        if(closed){
            throw new RuntimeStoreException("The store is closed");
        }
    }

    protected synchronized void initialize() throws IOException{
    	if( closed )
    		throw new IOException("Store has been closed.");
        if(!initialized){
            initialized=true;
            directory=new File(name);
            directory.mkdirs();
            File ifile=new File(directory,"kaha.idx");
            indexManager=new IndexManager(ifile,mode);
            rootData=new DataManager(directory,"roots-data");
            defaultContainerManager=new DataManager(directory,DEFAULT_CONTAINER_NAME);
            dataManagers.put(DEFAULT_CONTAINER_NAME, defaultContainerManager);
            IndexItem mapRoot=new IndexItem();
            IndexItem listRoot=new IndexItem();
            if(indexManager.isEmpty()){
                mapRoot.setOffset(0);
                indexManager.updateIndex(mapRoot);
                listRoot.setOffset(IndexItem.INDEX_SIZE);
                indexManager.updateIndex(listRoot);
                indexManager.setLength(IndexItem.INDEX_SIZE*2);
            }else{
                mapRoot=indexManager.getIndex(0);
                listRoot=indexManager.getIndex(IndexItem.INDEX_SIZE);
            }
            mapsContainer=new IndexRootContainer(mapRoot,indexManager,rootData);
            listsContainer=new IndexRootContainer(listRoot,indexManager,rootData);
            rootData.consolidateDataFiles();
            for(Iterator i=mapsContainer.getKeys().iterator();i.hasNext();){
                ContainerId key=(ContainerId) i.next();
                DataManager dm = getDataManager(key.getDataContainerPrefix());
                IndexItem root=mapsContainer.getRoot(key);
                BaseContainerImpl container=new MapContainerImpl(key,root,indexManager,dm);
                container.expressDataInterest();
                maps.put(key.getKey(),container);
            }
            for(Iterator i=listsContainer.getKeys().iterator();i.hasNext();){
                ContainerId key=(ContainerId) i.next();
                DataManager dm = getDataManager(key.getDataContainerPrefix());
                IndexItem root=listsContainer.getRoot(key);
                BaseContainerImpl container=new ListContainerImpl(key,root,indexManager,dm);
                container.expressDataInterest();
                lists.put(key.getKey(),container);
            }
            for (Iterator i = dataManagers.values().iterator(); i.hasNext();){
                DataManager dm = (DataManager) i.next();
                dm.consolidateDataFiles();
            }
        }
    }
    
    protected DataManager getDataManager(String prefix){
        DataManager dm = (DataManager) dataManagers.get(prefix);
        if (dm == null){
            dm = new DataManager(directory,prefix);
            dataManagers.put(prefix,dm);
        }
        return dm;
    }
}
