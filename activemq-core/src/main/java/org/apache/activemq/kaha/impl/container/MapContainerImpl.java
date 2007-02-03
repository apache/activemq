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

package org.apache.activemq.kaha.impl.container;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.activemq.kaha.Store;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.DataManager;
import org.apache.activemq.kaha.impl.data.Item;
import org.apache.activemq.kaha.impl.index.Index;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexLinkedList;
import org.apache.activemq.kaha.impl.index.IndexManager;
import org.apache.activemq.kaha.impl.index.VMIndex;
import org.apache.activemq.kaha.impl.index.hash.HashIndex;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Implementation of a MapContainer
 * 
 * @version $Revision: 1.2 $
 */
public final class MapContainerImpl extends BaseContainerImpl implements MapContainer{

    private static final Log log=LogFactory.getLog(MapContainerImpl.class);
    protected Index index;
    protected Marshaller keyMarshaller=Store.ObjectMarshaller;
    protected Marshaller valueMarshaller=Store.ObjectMarshaller;
    protected File directory;

    public MapContainerImpl(File directory,ContainerId id,IndexItem root,IndexManager indexManager,DataManager dataManager,
            Store.IndexType indexType){
        super(id,root,indexManager,dataManager,indexType);
        this.directory = directory;
    }

    public synchronized void init() {
        super.init();
        if(index==null){
            if(indexType.equals(Store.IndexType.PERSISTENT)){
                String name = containerId.getDataContainerName() + "_" + containerId.getKey();
                try{
                    this.index=new HashIndex(directory, name , indexManager);
                }catch(IOException e){
                    log.error("Failed to create HashIndex",e);
                    throw new RuntimeException(e);
                }
            }else{
                this.index=new VMIndex();
            }
        }
        index.setKeyMarshaller(keyMarshaller);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#load()
     */
    public synchronized void load(){
        checkClosed();
        if(!loaded){
            if(!loaded){
                loaded=true;
                try{
                    init();
                    index.load();
                    long nextItem=root.getNextItem();
                    while(nextItem!=Item.POSITION_NOT_SET){
                        IndexItem item=indexManager.getIndex(nextItem);
                        StoreLocation data=item.getKeyDataItem();
                        Object key=dataManager.readItem(keyMarshaller,data);
                        if(index.isTransient()){
                            index.store(key,item);
                        }
                        indexList.add(item);
                        nextItem=item.getNextItem();
                    }
                }catch(IOException e){
                    log.error("Failed to load container "+getId(),e);
                    throw new RuntimeStoreException(e);
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#unload()
     */
    public synchronized void unload(){
        checkClosed();
        if(loaded){
            loaded=false;
            try{
                index.unload();
            }catch(IOException e){
                log.warn("Failed to unload the index",e);
            }
            indexList.clear();
        }
    }

    public synchronized void setKeyMarshaller(Marshaller keyMarshaller){
        checkClosed();
        this.keyMarshaller=keyMarshaller;
        if(index!=null){
            index.setKeyMarshaller(keyMarshaller);
        }
    }

    public synchronized void setValueMarshaller(Marshaller valueMarshaller){
        checkClosed();
        this.valueMarshaller=valueMarshaller;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#size()
     */
    public synchronized int size(){
        load();
        return indexList.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#isEmpty()
     */
    public synchronized boolean isEmpty(){
        load();
        return indexList.isEmpty();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#containsKey(java.lang.Object)
     */
    public synchronized boolean containsKey(Object key){
        load();
        try{
            return index.containsKey(key);
        }catch(IOException e){
            log.error("Failed trying to find key: "+key,e);
            throw new RuntimeException(e);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#get(java.lang.Object)
     */
    public synchronized Object get(Object key){
        load();
        Object result=null;
        StoreEntry item=null;
        try{
            item=(StoreEntry)index.get(key);
        }catch(IOException e){
            log.error("Failed trying to get key: "+key,e);
            throw new RuntimeException(e);
        }
        if(item!=null){
            result=getValue(item);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#containsValue(java.lang.Object)
     */
    public synchronized boolean containsValue(Object o){
        load();
        boolean result=false;
        if(o!=null){
            IndexItem item=indexList.getFirst();
            while(item!=null){
                Object value=getValue(item);
                if(value!=null&&value.equals(o)){
                    result=true;
                    break;
                }
                item=indexList.getNextEntry(item);
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#putAll(java.util.Map)
     */
    public synchronized void putAll(Map t){
        load();
        if(t!=null){
            for(Iterator i=t.entrySet().iterator();i.hasNext();){
                Map.Entry entry=(Map.Entry)i.next();
                put(entry.getKey(),entry.getValue());
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#keySet()
     */
    public synchronized Set keySet(){
        load();
        return new ContainerKeySet(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#values()
     */
    public synchronized Collection values(){
        load();
        return new ContainerValueCollection(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#entrySet()
     */
    public synchronized Set entrySet(){
        load();
        return new ContainerEntrySet(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#put(java.lang.Object, java.lang.Object)
     */
    public synchronized Object put(Object key,Object value){
        load();
        Object result=remove(key);;
        IndexItem item=write(key,value);
        try{
            index.store(key,item);
        }catch(IOException e){
            log.error("Failed trying to insert key: "+key,e);
            throw new RuntimeException(e);
        }
        indexList.add(item);
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#remove(java.lang.Object)
     */
    public synchronized Object remove(Object key){
        load();
        try{
            Object result=null;
            IndexItem item=(IndexItem)index.get(key);
            if(item!=null){
                // refresh the index
                item=(IndexItem)indexList.refreshEntry(item);
                index.remove(key);
                result=getValue(item);
                IndexItem prev=indexList.getPrevEntry(item);
                IndexItem next=indexList.getNextEntry(item);
                indexList.remove(item);
                delete(item,prev,next);
            }
            return result;
        }catch(IOException e){
            log.error("Failed trying to remove key: "+key,e);
            throw new RuntimeException(e);
        }
    }

    public synchronized boolean removeValue(Object o){
        load();
        boolean result=false;
        if(o!=null){
            IndexItem item=indexList.getFirst();
            while(item!=null){
                Object value=getValue(item);
                if(value!=null&&value.equals(o)){
                    result=true;
                    // find the key
                    Object key=getKey(item);
                    if(key!=null){
                        remove(key);
                    }
                    break;
                }
                item=indexList.getNextEntry(item);
            }
        }
        return result;
    }

    protected synchronized void remove(IndexItem item){
        Object key=getKey(item);
        if(key!=null){
            remove(key);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#clear()
     */
    public synchronized void clear(){
        checkClosed();
        loaded=true;
        if(index!=null){
            try{
                index.clear();
            }catch(IOException e){
                log.error("Failed trying clear index",e);
                throw new RuntimeException(e);
            }
        }
        super.clear();
        doClear();
    }

    /**
     * Add an entry to the Store Map
     * 
     * @param key
     * @param value
     * @return the StoreEntry associated with the entry
     */
    public synchronized StoreEntry place(Object key,Object value){
        load();
        try{
            if(index.containsKey(key)){
                remove(key);
            }
            IndexItem item=write(key,value);
            index.store(key,item);
            indexList.add(item);
            return item;
        }catch(IOException e){
            log.error("Failed trying to palce key: "+key,e);
            throw new RuntimeException(e);
        }
    }

    /**
     * Remove an Entry from ther Map
     * 
     * @param entry
     * @throws IOException
     */
    public synchronized void remove(StoreEntry entry){
        load();
        IndexItem item=(IndexItem)entry;
        if(item!=null){
            Object key=getKey(item);
            try{
                index.remove(key);
            }catch(IOException e){
                log.error("Failed trying to remove entry: "+entry,e);
                throw new RuntimeException(e);
            }
            IndexItem prev=indexList.getPrevEntry(item);
            IndexItem next=indexList.getNextEntry(item);
            indexList.remove(item);
            delete(item,prev,next);
        }
    }

    /**
     * Get the value from it's location
     * 
     * @param item
     * @return the value associated with the store entry
     */
    public synchronized Object getValue(StoreEntry item){
        load();
        Object result=null;
        if(item!=null){
            try{
                // ensure this value is up to date
                // item=indexList.getEntry(item);
                StoreLocation data=item.getValueDataItem();
                result=dataManager.readItem(valueMarshaller,data);
            }catch(IOException e){
                log.error("Failed to get value for "+item,e);
                throw new RuntimeStoreException(e);
            }
        }
        return result;
    }

    /**
     * Get the Key object from it's location
     * 
     * @param item
     * @return the Key Object associated with the StoreEntry
     */
    public synchronized Object getKey(StoreEntry item){
        load();
        Object result=null;
        if(item!=null){
            try{
                StoreLocation data=item.getKeyDataItem();
                result=dataManager.readItem(keyMarshaller,data);
            }catch(IOException e){
                log.error("Failed to get key for "+item,e);
                throw new RuntimeStoreException(e);
            }
        }
        return result;
    }

    protected IndexLinkedList getItemList(){
        return indexList;
    }

    protected synchronized IndexItem write(Object key,Object value){
        IndexItem index=null;
        try{
            if(key!=null){
                index=indexManager.createNewIndex();
                StoreLocation data=dataManager.storeDataItem(keyMarshaller,key);
                index.setKeyData(data);
            }
            if(value!=null){
                StoreLocation data=dataManager.storeDataItem(valueMarshaller,value);
                index.setValueData(data);
            }
            IndexItem prev=indexList.getLast();
            prev=prev!=null?prev:indexList.getRoot();
            IndexItem next=indexList.getNextEntry(prev);
            prev.setNextItem(index.getOffset());
            index.setPreviousItem(prev.getOffset());
            updateIndexes(prev);
            if(next!=null){
                next.setPreviousItem(index.getOffset());
                index.setNextItem(next.getOffset());
                updateIndexes(next);
            }
            storeIndex(index);
        }catch(IOException e){
            log.error("Failed to write "+key+" , "+value,e);
            throw new RuntimeStoreException(e);
        }
        return index;
    }
}
