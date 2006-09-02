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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
/**
 * Optimized Store writer
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class KahaStore implements Store{
    
    private static final String DEFAULT_CONTAINER_NAME = "kaha";
    private static final Log log=LogFactory.getLog(KahaStore.class);
    private File directory;

    protected IndexRootContainer mapsContainer;
    protected IndexRootContainer listsContainer;
    private Map lists=new ConcurrentHashMap();
    private Map maps=new ConcurrentHashMap();
    
    private Map dataManagers = new ConcurrentHashMap();
    private Map indexManagers = new ConcurrentHashMap();
    protected IndexManager rootIndexManager; //contains all the root indexes
    
    private boolean closed=false;
    private String name;
    private String mode;
    private boolean initialized;
    private boolean logIndexChanges=false;
    private long maxDataFileLength = DataManager.MAX_FILE_LENGTH;

    public KahaStore(String name,String mode) throws IOException{
        this.name=name;
        this.mode=mode;
        directory=new File(name);
        directory.mkdirs();
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
        boolean result=true;
        if (initialized){
            clear();
            
            for(Iterator iter=indexManagers.values().iterator();iter.hasNext();){
                IndexManager im=(IndexManager) iter.next();
                result&=im.delete();
                iter.remove();
            }
            for(Iterator iter=dataManagers.values().iterator();iter.hasNext();){
                DataManager dm=(DataManager) iter.next();
                result&=dm.delete();
                iter.remove();
            }
        }
       
        if(directory!=null&&directory.isDirectory()){
            File[] files=directory.listFiles();
            if(files!=null){
                for(int i=0;i<files.length;i++){
                    File file=files[i];
                    if(!file.isDirectory()){
                        result&=file.delete();
                    }
                }
            }
        }
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
       
        MapContainerImpl result=(MapContainerImpl) maps.get(id);
        if(result==null){
            
            DataManager dm = getDataManager(dataContainerName);
            IndexManager im = getIndexManager(dm, dataContainerName);
            
            ContainerId containerId = new ContainerId();
            containerId.setKey(id);
            containerId.setDataContainerName(dataContainerName);

            IndexItem root=mapsContainer.getRoot(containerId);
            if( root == null ) {
                root=mapsContainer.addRoot(containerId);
            }
            result=new MapContainerImpl(containerId,root,rootIndexManager,im,dm);
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
        return getListContainer(id,DEFAULT_CONTAINER_NAME);
    }
    
    public synchronized ListContainer getListContainer(Object id, String containerName) throws IOException{
        initialize();
       
        ListContainerImpl result=(ListContainerImpl) lists.get(id);
        if(result==null){
            DataManager dm = getDataManager(containerName);
            IndexManager im = getIndexManager(dm, containerName);
            ContainerId containerId = new ContainerId();
            containerId.setKey(id);
            containerId.setDataContainerName(containerName);
            IndexItem root=listsContainer.getRoot(containerId);
            if( root == null ) {
                root=listsContainer.addRoot(containerId);
            }
            result=new ListContainerImpl(containerId,root,rootIndexManager,im,dm);
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
            
            log.info("Kaha Store using data directory " + directory);
            DataManager defaultDM = getDataManager(DEFAULT_CONTAINER_NAME);
            rootIndexManager = getIndexManager(defaultDM, DEFAULT_CONTAINER_NAME);
            
            IndexItem mapRoot=new IndexItem();
            IndexItem listRoot=new IndexItem();
            if(rootIndexManager.isEmpty()){
                mapRoot.setOffset(0);
                rootIndexManager.updateIndex(mapRoot);
                listRoot.setOffset(IndexItem.INDEX_SIZE);
                rootIndexManager.updateIndex(listRoot);
                rootIndexManager.setLength(IndexItem.INDEX_SIZE*2);
            }else{
                mapRoot=rootIndexManager.getIndex(0);
                listRoot=rootIndexManager.getIndex(IndexItem.INDEX_SIZE);
            }
            mapsContainer=new IndexRootContainer(mapRoot,rootIndexManager,defaultDM);
            listsContainer=new IndexRootContainer(listRoot,rootIndexManager,defaultDM);

            for (Iterator i = dataManagers.values().iterator(); i.hasNext();){
                DataManager dm = (DataManager) i.next();
                dm.consolidateDataFiles();
            }
        }
    }
    
    protected DataManager getDataManager(String name) throws IOException {
        DataManager dm = (DataManager) dataManagers.get(name);
        if (dm == null){
            dm = new DataManager(directory,name);
            dm.setMaxFileLength(maxDataFileLength);
            recover(dm);
            dataManagers.put(name,dm);
        }
        return dm;
    }
    
    protected IndexManager getIndexManager(DataManager dm, String name) throws IOException {
        IndexManager im = (IndexManager) indexManagers.get(name);
        if( im == null ) {
            im = new IndexManager(directory,name,mode, logIndexChanges?dm:null);
            indexManagers.put(name,im);
        }
        return im;
    }

    private void recover(final DataManager dm) throws IOException {
        dm.recoverRedoItems( new RedoListener() {
            public void onRedoItem(DataItem item, Object o) throws Exception {
                RedoStoreIndexItem redo = (RedoStoreIndexItem) o;
                //IndexManager im = getIndexManager(dm, redo.getIndexName());
                IndexManager im = getIndexManager(dm, dm.getName());
                im.redo(redo);
            }
        });
    }

    public boolean isLogIndexChanges() {
        return logIndexChanges;
    }

    public void setLogIndexChanges(boolean logIndexChanges) {
        this.logIndexChanges = logIndexChanges;
    }

    /**
     * @return the maxDataFileLength
     */
    public long getMaxDataFileLength(){
        return maxDataFileLength;
    }

    /**
     * @param maxDataFileLength the maxDataFileLength to set
     */
    public void setMaxDataFileLength(long maxDataFileLength){
        this.maxDataFileLength=maxDataFileLength;
    }

}
