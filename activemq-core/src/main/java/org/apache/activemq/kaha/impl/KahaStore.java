/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.activemq.kaha.impl;

import java.io.File;
import java.io.IOException;
import java.nio.channels.FileLock;
import java.util.Date;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;
import org.apache.activemq.kaha.IndexTypes;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.container.BaseContainerImpl;
import org.apache.activemq.kaha.impl.container.ContainerId;
import org.apache.activemq.kaha.impl.container.ListContainerImpl;
import org.apache.activemq.kaha.impl.container.MapContainerImpl;
import org.apache.activemq.kaha.impl.data.DataManager;
import org.apache.activemq.kaha.impl.data.Item;
import org.apache.activemq.kaha.impl.data.RedoListener;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexManager;
import org.apache.activemq.kaha.impl.index.RedoStoreIndexItem;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Store Implementation
 * 
 * @version $Revision: 1.1.1.1 $
 */
public class KahaStore implements Store{

    private static final String DEFAULT_CONTAINER_NAME="kaha";
    private final static String PROPERTY_PREFIX="org.apache.activemq.kaha.Store";
    private final static boolean brokenFileLock="true".equals(System.getProperty(PROPERTY_PREFIX+".broken","false"));
    private final static boolean disableLocking="true".equals(System.getProperty(PROPERTY_PREFIX+"DisableLocking",
            "false"));
    //according to the String javadoc, all constant strings are interned so this will be the same object throughout the vm
    //and we can use it as a monitor for the lockset.
    private final static String LOCKSET_MONITOR = PROPERTY_PREFIX + ".Lock.Monitor";
    private static final Log log=LogFactory.getLog(KahaStore.class);
    private File directory;
    private IndexRootContainer mapsContainer;
    private IndexRootContainer listsContainer;
    private Map lists=new ConcurrentHashMap();
    private Map maps=new ConcurrentHashMap();
    private Map dataManagers=new ConcurrentHashMap();
    private Map indexManagers=new ConcurrentHashMap();
    private IndexManager rootIndexManager; // contains all the root indexes
    private boolean closed=false;
    private String mode;
    private boolean initialized;
    private boolean logIndexChanges=false;
    private long maxDataFileLength=DataManager.MAX_FILE_LENGTH;
    private FileLock lock;
    private String indexType=IndexTypes.DISK_INDEX;

    public KahaStore(String name,String mode) throws IOException{
        this.mode=mode;
        directory=new File(name);
        directory.mkdirs();
    }

    public synchronized void close() throws IOException{
        if(!closed){
            closed=true;
            if(initialized){
                unlock();
                for(Iterator iter=indexManagers.values().iterator();iter.hasNext();){
                    IndexManager im=(IndexManager)iter.next();
                    im.close();
                    iter.remove();
                }
                for(Iterator iter=dataManagers.values().iterator();iter.hasNext();){
                    DataManager dm=(DataManager)iter.next();
                    dm.close();
                    iter.remove();
                }
            }
        }
    }

    public synchronized void force() throws IOException{
        if(initialized){
            for(Iterator iter=indexManagers.values().iterator();iter.hasNext();){
                IndexManager im=(IndexManager)iter.next();
                im.force();
            }
            for(Iterator iter=dataManagers.values().iterator();iter.hasNext();){
                DataManager dm=(DataManager)iter.next();
                dm.force();
            }
        }
    }

    public synchronized void clear() throws IOException{
        initialize();
        for(Iterator i=maps.values().iterator();i.hasNext();){
            BaseContainerImpl container=(BaseContainerImpl)i.next();
            container.clear();
        }
        for(Iterator i=lists.values().iterator();i.hasNext();){
            BaseContainerImpl container=(BaseContainerImpl)i.next();
            container.clear();
        }
    }

    public synchronized boolean delete() throws IOException{
        boolean result=true;
        if(initialized){
            clear();
            for(Iterator iter=indexManagers.values().iterator();iter.hasNext();){
                IndexManager im=(IndexManager)iter.next();
                result&=im.delete();
                iter.remove();
            }
            for(Iterator iter=dataManagers.values().iterator();iter.hasNext();){
                DataManager dm=(DataManager)iter.next();
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
            log.info("Kaha Store deleted data directory "+directory);
        }
        initialized=false;
        return result;
    }

    public boolean doesMapContainerExist(Object id) throws IOException{
        return doesMapContainerExist(id,DEFAULT_CONTAINER_NAME);
    }

    public boolean doesMapContainerExist(Object id,String containerName) throws IOException{
        initialize();
        ContainerId containerId=new ContainerId();
        containerId.setKey(id);
        containerId.setDataContainerName(containerName);
        return maps.containsKey(containerId)||mapsContainer.doesRootExist(containerId);
    }

    public MapContainer getMapContainer(Object id) throws IOException{
        return getMapContainer(id,DEFAULT_CONTAINER_NAME);
    }

    public MapContainer getMapContainer(Object id,String containerName) throws IOException{
        return getMapContainer(id,containerName,indexType);
    }

    public synchronized MapContainer getMapContainer(Object id,String containerName,String indexType)
            throws IOException{
        initialize();
        ContainerId containerId=new ContainerId();
        containerId.setKey(id);
        containerId.setDataContainerName(containerName);
        MapContainerImpl result=(MapContainerImpl)maps.get(containerId);
        if(result==null){
            DataManager dm=getDataManager(containerName);
            IndexManager im=getIndexManager(dm,containerName);
            IndexItem root=mapsContainer.getRoot(im,containerId);
            if(root==null){
                root=mapsContainer.addRoot(im,containerId);
            }
            result=new MapContainerImpl(containerId,root,im,dm,indexType);
            maps.put(containerId,result);
        }
        return result;
    }

    public void deleteMapContainer(Object id) throws IOException{
        deleteMapContainer(id,DEFAULT_CONTAINER_NAME);
    }

    public void deleteMapContainer(Object id,String containerName) throws IOException{
        initialize();
        ContainerId containerId=new ContainerId();
        containerId.setKey(id);
        containerId.setDataContainerName(containerName);
        MapContainerImpl container=(MapContainerImpl)maps.remove(containerId);
        if(container!=null){
            container.clear();
            mapsContainer.removeRoot(container.getIndexManager(),containerId);
        }
    }

    public Set getMapContainerIds() throws IOException{
        initialize();
        Set set = new HashSet();
        for (Iterator i = mapsContainer.getKeys().iterator(); i.hasNext();) {
            ContainerId id = (ContainerId)i.next();
            set.add(id.getKey());
        }
        return set;
    }

    public boolean doesListContainerExist(Object id) throws IOException{
        return doesListContainerExist(id,DEFAULT_CONTAINER_NAME);
    }

    public boolean doesListContainerExist(Object id,String containerName) throws IOException{
        initialize();
        ContainerId containerId=new ContainerId();
        containerId.setKey(id);
        containerId.setDataContainerName(containerName);
        return lists.containsKey(containerId)||listsContainer.doesRootExist(containerId);
    }

    public ListContainer getListContainer(Object id) throws IOException{
        return getListContainer(id,DEFAULT_CONTAINER_NAME);
    }

    public ListContainer getListContainer(Object id,String containerName) throws IOException{
        return getListContainer(id,containerName,indexType);
    }

    public synchronized ListContainer getListContainer(Object id,String containerName,String indexType)
            throws IOException{
        initialize();
        ContainerId containerId=new ContainerId();
        containerId.setKey(id);
        containerId.setDataContainerName(containerName);
        ListContainerImpl result=(ListContainerImpl)lists.get(containerId);
        if(result==null){
            DataManager dm=getDataManager(containerName);
            IndexManager im=getIndexManager(dm,containerName);
            
            IndexItem root=listsContainer.getRoot(im,containerId);
            if(root==null){
                root=listsContainer.addRoot(im,containerId);
            }
            result=new ListContainerImpl(containerId,root,im,dm,indexType);
            lists.put(containerId,result);
        }
        return result;
    }

    public void deleteListContainer(Object id) throws IOException{
        deleteListContainer(id,DEFAULT_CONTAINER_NAME);
    }

    public void deleteListContainer(Object id,String containerName) throws IOException{
        initialize();
        ContainerId containerId=new ContainerId();
        containerId.setKey(id);
        containerId.setDataContainerName(containerName);
        ListContainerImpl container=(ListContainerImpl)lists.remove(containerId);
        if(container!=null){
            listsContainer.removeRoot(container.getIndexManager(),containerId);
            container.clear();
        }
    }

    public Set getListContainerIds() throws IOException{
        initialize();
        Set set = new HashSet();
        for (Iterator i = listsContainer.getKeys().iterator(); i.hasNext();) {
            ContainerId id = (ContainerId)i.next();
            set.add(id.getKey());
        }
        return set;
    }

    
    
    /**
     * @return the listsContainer
     */
    public IndexRootContainer getListsContainer(){
        return this.listsContainer;
    }

    
    /**
     * @return the mapsContainer
     */
    public IndexRootContainer getMapsContainer(){
        return this.mapsContainer;
    }

    public DataManager getDataManager(String name) throws IOException{
        DataManager dm=(DataManager)dataManagers.get(name);
        if(dm==null){
            dm=new DataManager(directory,name);
            dm.setMaxFileLength(maxDataFileLength);
            recover(dm);
            dataManagers.put(name,dm);
        }
        return dm;
    }

    public IndexManager getIndexManager(DataManager dm,String name) throws IOException{
        IndexManager im=(IndexManager)indexManagers.get(name);
        if(im==null){
            im=new IndexManager(directory,name,mode,logIndexChanges?dm:null);
            indexManagers.put(name,im);
        }
        return im;
    }

    private void recover(final DataManager dm) throws IOException{
        dm.recoverRedoItems(new RedoListener(){

            public void onRedoItem(StoreLocation item,Object o) throws Exception{
                RedoStoreIndexItem redo=(RedoStoreIndexItem)o;
                // IndexManager im = getIndexManager(dm, redo.getIndexName());
                IndexManager im=getIndexManager(dm,dm.getName());
                im.redo(redo);
            }
        });
    }

    public boolean isLogIndexChanges(){
        return logIndexChanges;
    }

    public void setLogIndexChanges(boolean logIndexChanges){
        this.logIndexChanges=logIndexChanges;
    }

    /**
     * @return the maxDataFileLength
     */
    public long getMaxDataFileLength(){
        return maxDataFileLength;
    }

    /**
     * @param maxDataFileLength
     *            the maxDataFileLength to set
     */
    public void setMaxDataFileLength(long maxDataFileLength){
        this.maxDataFileLength=maxDataFileLength;
    }

    /**
     * @see org.apache.activemq.kaha.IndexTypes
     * @return the default index type
     */
    public String getIndexType(){
        return indexType;
    }

    /**
     * Set the default index type
     * 
     * @param type
     * @see org.apache.activemq.kaha.IndexTypes
     */
    public void setIndexType(String type){
        if(type==null||(!type.equals(IndexTypes.DISK_INDEX)&&!type.equals(IndexTypes.IN_MEMORY_INDEX))){
            throw new RuntimeException("Unknown IndexType: "+type);
        }
        this.indexType=type;
    }
    
    public synchronized void initialize() throws IOException{
        if(closed)
            throw new IOException("Store has been closed.");
        if(!initialized){
            initialized=true;
            log.info("Kaha Store using data directory "+directory);
            DataManager defaultDM=getDataManager(DEFAULT_CONTAINER_NAME);
            rootIndexManager=getIndexManager(defaultDM,DEFAULT_CONTAINER_NAME);
            IndexItem mapRoot=new IndexItem();
            IndexItem listRoot=new IndexItem();
            if(rootIndexManager.isEmpty()){
                mapRoot.setOffset(0);
                rootIndexManager.storeIndex(mapRoot);
                listRoot.setOffset(IndexItem.INDEX_SIZE);
                rootIndexManager.storeIndex(listRoot);
                rootIndexManager.setLength(IndexItem.INDEX_SIZE*2);
            }else{
                mapRoot=rootIndexManager.getIndex(0);
                listRoot=rootIndexManager.getIndex(IndexItem.INDEX_SIZE);
            }
            lock();
            mapsContainer=new IndexRootContainer(mapRoot,rootIndexManager,defaultDM);
            listsContainer=new IndexRootContainer(listRoot,rootIndexManager,defaultDM);
            /**
             * Add interest in data files - then consolidate them
             */
            generateInterestInMapDataFiles();
            generateInterestInListDataFiles();
            for(Iterator i=dataManagers.values().iterator();i.hasNext();){
                DataManager dm=(DataManager)i.next();
                dm.consolidateDataFiles();
            }
        }
    }

    private void lock() throws IOException {
        synchronized (LOCKSET_MONITOR) {
            if (!disableLocking && directory != null && lock == null) {
                String key = getPropertyKey();
                String property = System.getProperty(key);
                if (null == property) {
                    if (!brokenFileLock) {
                        lock = rootIndexManager.getLock();
                        if (lock == null) {
                            throw new StoreLockedExcpetion("Kaha Store " + directory.getName() + "  is already opened by another application");
                        } else
                            System.setProperty(key, new Date().toString());
                    }
                } else { //already locked
                    throw new StoreLockedExcpetion("Kaha Store " + directory.getName() + " is already opened by this application.");
                }
            }
        }
    }

    private void unlock() throws IOException {
        synchronized (LOCKSET_MONITOR) {
            if (!disableLocking && (null != directory) && (null != lock)) {
                System.getProperties().remove(getPropertyKey());
                if (lock.isValid()) {
                    lock.release();
                }
                lock = null;
            }
        }
    }

    private String getPropertyKey() throws IOException {
        //Is replaceAll() needed?  Should test without it.
        return getClass().getName() + ".lock." + directory.getCanonicalPath();
    }

    /**
     * scans the directory and builds up the IndexManager and DataManager
     * @throws IOException 
     */
    private void generateInterestInListDataFiles() throws IOException {
        for (Iterator i = listsContainer.getKeys().iterator(); i.hasNext();) {
            ContainerId id = (ContainerId)i.next();
            DataManager dm = getDataManager(id.getDataContainerName());
            IndexManager im = getIndexManager(dm,id.getDataContainerName());
            IndexItem theRoot=listsContainer.getRoot(im,id);
            long nextItem=theRoot.getNextItem();
            while(nextItem!=Item.POSITION_NOT_SET){
                IndexItem item=im.getIndex(nextItem);
                item.setOffset(nextItem);
                dm.addInterestInFile(item.getKeyFile());
                dm.addInterestInFile(item.getValueFile());
                nextItem=item.getNextItem();
            }
            
        }
    }
    
    /**
     * scans the directory and builds up the IndexManager and DataManager
     * @throws IOException 
     */
    private void generateInterestInMapDataFiles() throws IOException {
        for (Iterator i = mapsContainer.getKeys().iterator(); i.hasNext();) {
            ContainerId id = (ContainerId)i.next();
            DataManager dm = getDataManager(id.getDataContainerName());
            IndexManager im = getIndexManager(dm,id.getDataContainerName());
            IndexItem theRoot=mapsContainer.getRoot(im,id);
            long nextItem=theRoot.getNextItem();
            while(nextItem!=Item.POSITION_NOT_SET){
                IndexItem item=im.getIndex(nextItem);
                item.setOffset(nextItem);
                dm.addInterestInFile(item.getKeyFile());
                dm.addInterestInFile(item.getValueFile());
                nextItem=item.getNextItem();
            }
            
        }
    }

    
   
}
