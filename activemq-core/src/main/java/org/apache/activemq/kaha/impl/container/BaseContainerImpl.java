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

package org.apache.activemq.kaha.impl.container;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.activemq.kaha.IndexTypes;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.impl.data.DataManager;
import org.apache.activemq.kaha.impl.data.Item;
import org.apache.activemq.kaha.impl.index.DiskIndexLinkedList;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexLinkedList;
import org.apache.activemq.kaha.impl.index.IndexManager;
import org.apache.activemq.kaha.impl.index.VMIndexLinkedList;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of a ListContainer
 * 
 * @version $Revision: 1.2 $
 */
public abstract class BaseContainerImpl{

    private static final Log log=LogFactory.getLog(BaseContainerImpl.class);
    protected IndexItem root;
    protected IndexLinkedList indexList;
    protected IndexManager indexManager;
    protected DataManager dataManager;
    protected ContainerId containerId;
    protected boolean loaded=false;
    protected boolean closed=false;
    protected boolean initialized=false;
    protected String indexType;

    protected BaseContainerImpl(ContainerId id,IndexItem root,IndexManager indexManager,
            DataManager dataManager,String indexType){
        this.containerId=id;
        this.root=root;
        this.indexManager=indexManager;
        this.dataManager=dataManager;
        this.indexType = indexType;
        if (indexType == null || (!indexType.equals(IndexTypes.DISK_INDEX) && !indexType.equals(IndexTypes.IN_MEMORY_INDEX))) {
            throw new RuntimeException("Unknown IndexType: " + indexType);
        }
    }

    public ContainerId getContainerId(){
        return containerId;
    }

    public synchronized void init(){
        if(!initialized){
            if(!initialized){
                initialized=true;
                if(this.indexList==null){
                    if(indexType.equals(IndexTypes.DISK_INDEX)){
                        this.indexList=new DiskIndexLinkedList(indexManager,root);
                    }else{
                        this.indexList=new VMIndexLinkedList(root);
                    }
                }
            }
        }
    }

    
    public synchronized void clear(){
        if(indexList!=null){
            indexList.clear();
        }       
    }
    /**
     * @return the indexList
     */
    public IndexLinkedList getList(){
        return indexList;
    }

    /**
     * @param indexList the indexList to set
     */
    public void setList(IndexLinkedList indexList){
        this.indexList=indexList;
    }

    public abstract void unload();

    public abstract void load();

    public abstract int size();

    protected abstract Object getValue(StoreEntry currentItem);

    protected abstract void remove(IndexItem currentItem);

    protected synchronized final IndexLinkedList getInternalList(){
        return indexList;
    }

    public synchronized final void close(){
        unload();
        closed=true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#isLoaded()
     */
    public synchronized final boolean isLoaded(){
        checkClosed();
        return loaded;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#getId()
     */
    public final Object getId(){
        checkClosed();
        return containerId.getKey();
    }
    
    public DataManager getDataManager(){
        return dataManager;
    }

    
    public IndexManager getIndexManager(){
        return indexManager;
    }

    public synchronized final void expressDataInterest() throws IOException{
        long nextItem=root.getNextItem();
        while(nextItem!=Item.POSITION_NOT_SET){
            IndexItem item=indexManager.getIndex(nextItem);
            item.setOffset(nextItem);
            dataManager.addInterestInFile(item.getKeyFile());
            dataManager.addInterestInFile(item.getValueFile());
            nextItem=item.getNextItem();
        }
    }

    protected final void doClear(){
        checkClosed();
        loaded=true;
        List indexList=new ArrayList();
        try{
            init();
            long nextItem=root.getNextItem();
            while(nextItem!=Item.POSITION_NOT_SET){
                IndexItem item=new IndexItem();
                item.setOffset(nextItem);
                indexList.add(item);
                nextItem=item.getNextItem();
            }
            root.setNextItem(Item.POSITION_NOT_SET);
            storeIndex(root);
            for(int i=0;i<indexList.size();i++){
                IndexItem item=(IndexItem)indexList.get(i);
                dataManager.removeInterestInFile(item.getKeyFile());
                dataManager.removeInterestInFile(item.getValueFile());
                indexManager.freeIndex(item);
            }
            indexList.clear();
        }catch(IOException e){
            log.error("Failed to clear Container "+getId(),e);
            throw new RuntimeStoreException(e);
        }
    }

    protected final void delete(IndexItem key,IndexItem prev,IndexItem next){
        try{
            dataManager.removeInterestInFile(key.getKeyFile());
            dataManager.removeInterestInFile(key.getValueFile());
            prev=prev==null?root:prev;
            next=next!=root?next:null;
            if(next!=null){
                prev.setNextItem(next.getOffset());
                next.setPreviousItem(prev.getOffset());
                updateIndexes(next);
            }else{
                prev.setNextItem(Item.POSITION_NOT_SET);
            }
            updateIndexes(prev);
            indexManager.freeIndex(key);
        }catch(IOException e){
            log.error("Failed to delete "+key,e);
            throw new RuntimeStoreException(e);
        }
    }

    protected final void checkClosed(){
        if(closed){
            throw new RuntimeStoreException("The store is closed");
        }
    }

    protected void storeIndex(IndexItem item) throws IOException{
        indexManager.storeIndex(item);
    }
    
    protected void updateIndexes(IndexItem item) throws IOException{
        indexManager.updateIndexes(item);
    }

    protected final boolean isRoot(StoreEntry item){
        return item!=null&&root!=null&&(root==item||root.getOffset()==item.getOffset());
        // return item != null && indexRoot != null && indexRoot == item;
    }

    
   
}
