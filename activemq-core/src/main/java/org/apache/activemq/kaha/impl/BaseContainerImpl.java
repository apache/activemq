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
package org.apache.activemq.kaha.impl;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.activemq.kaha.RuntimeStoreException;
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
    protected IndexLinkedList list;
    protected IndexManager indexManager;
    protected DataManager dataManager;
    protected ContainerId containerId;
    protected boolean loaded=false;
    protected boolean closed=false;
    protected final Object mutex=new Object();

    protected BaseContainerImpl(ContainerId id,IndexItem root,IndexManager indexManager,DataManager dataManager){
        this.containerId=id;
        this.root=root;
        this.indexManager=indexManager;
        this.dataManager=dataManager;
        this.list=new IndexLinkedList(root);
    }
    
    ContainerId getContainerId(){
        return containerId;
    }

    public abstract void unload();

    public abstract void load();

    public abstract int size();

    public abstract void clear();

    protected abstract Object getValue(IndexItem currentItem);

    protected abstract void remove(IndexItem currentItem);

    protected final IndexLinkedList getInternalList(){
        return list;
    }

    public final void close(){
        unload();
        closed=true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#isLoaded()
     */
    public final boolean isLoaded(){
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

    protected final void expressDataInterest() throws IOException{
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
        synchronized(mutex){
            loaded=true;
            synchronized(mutex){
                List list=new ArrayList();
                try{
                    long nextItem=root.getNextItem();
                    while(nextItem!=Item.POSITION_NOT_SET){
                        IndexItem item=new IndexItem();
                        item.setOffset(nextItem);
                        list.add(item);
                        nextItem=item.getNextItem();
                    }
                    root.setNextItem(Item.POSITION_NOT_SET);
                    indexManager.updateIndex(root);
                    for(int i=0;i<list.size();i++){
                        IndexItem item=(IndexItem) list.get(i);
                        dataManager.removeInterestInFile(item.getKeyFile());
                        dataManager.removeInterestInFile(item.getValueFile());
                        indexManager.freeIndex(item);
                    }
                    list.clear();
                }catch(IOException e){
                    log.error("Failed to clear Container "+getId(),e);
                    throw new RuntimeStoreException(e);
                }
            }
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
                indexManager.updateIndex(next);
            }else{
                prev.setNextItem(Item.POSITION_NOT_SET);
            }
            indexManager.updateIndex(prev);
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
}
