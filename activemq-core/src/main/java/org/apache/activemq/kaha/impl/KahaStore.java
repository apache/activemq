/**
 * 
 * Copyright 2005-2006 The Apache Software Foundation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
/**
 * Optimized writes to a RandomAcessFile
 * 
 * @version $Revision: 1.1.1.1 $
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
    
    private static final String DEFAULT_DATA_CONTAINER_NAME = "kaha-data.";
    private static final String DEFAULT_INDEX_CONTAINER_NAME = "kaha-index.";
    
    private File directory;

    private IndexRootContainer mapsContainer;
    private IndexRootContainer listsContainer;
    private Map lists=new ConcurrentHashMap();
    private Map maps=new ConcurrentHashMap();
    
    private Map dataManagers = new ConcurrentHashMap();
    private Map indexManagers = new ConcurrentHashMap();
    
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
                
                for (Iterator iter = indexManagers.values().iterator(); iter.hasNext();) {
                    IndexManager im = (IndexManager) iter.next();
                    im.close();
                    iter.remove();
                }

                for (Iterator iter = dataManagers.values().iterator(); iter.hasNext();) {
                    DataManager dm = (DataManager) iter.next();
                    dm.close();
                    iter.remove();
                }
                
            }
        }
    }

    public synchronized void force() throws IOException{
        if(initialized){

            for (Iterator iter = indexManagers.values().iterator(); iter.hasNext();) {
                IndexManager im = (IndexManager) iter.next();
                im.force();
            }

            for (Iterator iter = dataManagers.values().iterator(); iter.hasNext();) {
                DataManager dm = (DataManager) iter.next();
                dm.force();
            }
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
        boolean result=true;
        
        for (Iterator iter = indexManagers.values().iterator(); iter.hasNext();) {
            IndexManager im = (IndexManager) iter.next();
            result &= im.delete();
            iter.remove();
        }
        
        for (Iterator iter = dataManagers.values().iterator(); iter.hasNext();) {
            DataManager dm = (DataManager) iter.next();
            result &= dm.delete();
            iter.remove();
        }
        
        initialized=false;
        return result;
    }

    public boolean doesMapContainerExist(Object id) throws IOException{
        initialize();
        return maps.containsKey(id);
    }

    public MapContainer getMapContainer(Object id) throws IOException{
        return getMapContainer(id, DEFAULT_DATA_CONTAINER_NAME);
    }
    
    public synchronized MapContainer getMapContainer(Object id, String dataContainerName) throws IOException{
        initialize();
       
        MapContainerImpl result=(MapContainerImpl) maps.get(id);
        if(result==null){
            
            DataManager dm = getDataManager(dataContainerName);
            IndexManager im = getIndexManager(DEFAULT_INDEX_CONTAINER_NAME);
            
            ContainerId containerId = new ContainerId();
            containerId.setKey(id);
            containerId.setDataContainerPrefix(dataContainerName);

            IndexItem root=mapsContainer.getRoot(containerId);
            if( root == null ) {
                root=mapsContainer.addRoot(containerId);
            }
            result=new MapContainerImpl(containerId,root,im,dm);
            result.expressDataInterest();
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
        return getListContainer(id,DEFAULT_DATA_CONTAINER_NAME);
    }
    
    public synchronized ListContainer getListContainer(Object id, String dataContainerName) throws IOException{
        initialize();
       
        ListContainerImpl result=(ListContainerImpl) lists.get(id);
        if(result==null){
            DataManager dm = getDataManager(dataContainerName);
            IndexManager im = getIndexManager(DEFAULT_INDEX_CONTAINER_NAME);
            ContainerId containerId = new ContainerId();
            containerId.setKey(id);
            containerId.setDataContainerPrefix(dataContainerName);
            IndexItem root=listsContainer.getRoot(containerId);
            if( root == null ) {
                root=listsContainer.addRoot(containerId);
            }
            result=new ListContainerImpl(containerId,root,im,dm);
            result.expressDataInterest();
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
            
            DataManager rootData = getDataManager(DEFAULT_DATA_CONTAINER_NAME);
            IndexManager rootIndex = getIndexManager(DEFAULT_INDEX_CONTAINER_NAME);
            
            IndexItem mapRoot=new IndexItem();
            IndexItem listRoot=new IndexItem();
            if(rootIndex.isEmpty()){
                mapRoot.setOffset(0);
                rootIndex.updateIndex(mapRoot);
                listRoot.setOffset(IndexItem.INDEX_SIZE);
                rootIndex.updateIndex(listRoot);
                rootIndex.setLength(IndexItem.INDEX_SIZE*2);
            }else{
                mapRoot=rootIndex.getIndex(0);
                listRoot=rootIndex.getIndex(IndexItem.INDEX_SIZE);
            }
            mapsContainer=new IndexRootContainer(mapRoot,rootIndex,rootData);
            listsContainer=new IndexRootContainer(listRoot,rootIndex,rootData);

            for (Iterator i = dataManagers.values().iterator(); i.hasNext();){
                DataManager dm = (DataManager) i.next();
                dm.consolidateDataFiles();
            }
        }
    }
    
    protected DataManager getDataManager(String prefix) throws IOException {
        DataManager dm = (DataManager) dataManagers.get(prefix);
        if (dm == null){
            dm = new DataManager(directory,prefix);
            dataManagers.put(prefix,dm);
        }
        return dm;
    }
    
    protected IndexManager getIndexManager(String index_name) throws IOException {
        IndexManager im = (IndexManager) indexManagers.get(index_name);
        if( im == null ) {
            File ifile=new File(directory,index_name+".idx");
            im = new IndexManager(ifile,mode);
            indexManagers.put(index_name,im);
        }
        return im;
    }

}
