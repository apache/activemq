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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.kaha.MapContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.ObjectMarshaller;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Implementation of a MapContainer
 * 
 * @version $Revision: 1.2 $
 */
final class MapContainerImpl extends BaseContainerImpl implements MapContainer{
    private static final Log log=LogFactory.getLog(MapContainerImpl.class);
    protected Map map=new HashMap();
    protected Map valueToKeyMap=new HashMap();
    protected Marshaller keyMarshaller=new ObjectMarshaller();
    protected Marshaller valueMarshaller=new ObjectMarshaller();

    protected MapContainerImpl(ContainerId id,IndexItem root,IndexManager indexManager,DataManager dataManager){
        super(id,root,indexManager,dataManager);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#load()
     */
    public void load(){
        checkClosed();
        if(!loaded){
            synchronized(mutex){
                if(!loaded){
                    loaded=true;
                    try{
                        long nextItem=root.getNextItem();
                        while(nextItem!=Item.POSITION_NOT_SET){
                            IndexItem item=indexManager.getIndex(nextItem);
                            DataItem data=item.getKeyDataItem();
                            Object key=dataManager.readItem(keyMarshaller,data);
                            map.put(key,item);
                            valueToKeyMap.put(item,key);
                            list.add(item);
                            nextItem=item.getNextItem();
                        }
                    }catch(IOException e){
                        log.error("Failed to load container "+getId(),e);
                        throw new RuntimeStoreException(e);
                    }
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#unload()
     */
    public void unload(){
        checkClosed();
        if(loaded){
            loaded=false;
            synchronized(mutex){
                map.clear();
                valueToKeyMap.clear();
                list.clear();
            }
        }
    }

    public void setKeyMarshaller(Marshaller keyMarshaller){
        checkClosed();
        this.keyMarshaller=keyMarshaller;
    }

    public void setValueMarshaller(Marshaller valueMarshaller){
        checkClosed();
        this.valueMarshaller=valueMarshaller;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#size()
     */
    public int size(){
        load();
        return map.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#isEmpty()
     */
    public boolean isEmpty(){
        load();
        return map.isEmpty();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key){
        load();
        synchronized(mutex){
            return map.containsKey(key);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#get(java.lang.Object)
     */
    public Object get(Object key){
        load();
        Object result=null;
        IndexItem item=null;
        synchronized(mutex){
            item=(IndexItem) map.get(key);
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
    public boolean containsValue(Object o){
        load();
        boolean result=false;
        if(o!=null){
            synchronized(list){
                IndexItem item=list.getFirst();
                while(item!=null){
                    Object value=getValue(item);
                    if(value!=null&&value.equals(o)){
                        result=true;
                        break;
                    }
                    item=list.getNextEntry(item);
                }
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#putAll(java.util.Map)
     */
    public void putAll(Map t){
        load();
        if(t!=null){
            synchronized(mutex){
                for(Iterator i=t.entrySet().iterator();i.hasNext();){
                    Map.Entry entry=(Map.Entry) i.next();
                    put(entry.getKey(),entry.getValue());
                }
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#keySet()
     */
    public Set keySet(){
        load();
        return new ContainerKeySet(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#values()
     */
    public Collection values(){
        load();
        return new ContainerValueCollection(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#entrySet()
     */
    public Set entrySet(){
        load();
        return new ContainerEntrySet(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#put(java.lang.Object, java.lang.Object)
     */
    public Object put(Object key,Object value){
        load();
        Object result=null;
        synchronized(mutex){
            if(map.containsKey(key)){
                result=remove(key);
            }
            IndexItem item=write(key,value);
            map.put(key,item);
            valueToKeyMap.put(item,key);
            list.add(item);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#remove(java.lang.Object)
     */
    public Object remove(Object key){
        load();
        Object result=null;
        synchronized(mutex){
            IndexItem item=(IndexItem) map.get(key);
            if(item!=null){
                map.remove(key);
                valueToKeyMap.remove(item);
                result=getValue(item);
                IndexItem prev=list.getPrevEntry(item);
                prev=prev!=null?prev:root;
                IndexItem next=list.getNextEntry(item);
                list.remove(item);
                delete(item,prev,next);
            }
        }
        return result;
    }

    public boolean removeValue(Object o){
        load();
        boolean result=false;
        if(o!=null){
            synchronized(mutex){
                IndexItem item=list.getFirst();
                while(item!=null){
                    Object value=getValue(item);
                    if(value!=null&&value.equals(o)){
                        result=true;
                        // find the key
                        Object key=valueToKeyMap.get(item);
                        if(key!=null){
                            remove(key);
                        }
                        break;
                    }
                    item=list.getNextEntry(item);
                }
            }
        }
        return result;
    }

    protected void remove(IndexItem item){
        Object key=valueToKeyMap.get(item);
        if(key!=null){
            remove(key);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#clear()
     */
    public void clear(){
        checkClosed();
        synchronized(mutex){
            loaded=true;
            synchronized(mutex){
                map.clear();
                valueToKeyMap.clear();
                list.clear();// going to re-use this
                doClear();
            }
        }
    }

    protected Set getInternalKeySet(){
        return new HashSet(map.keySet());
    }

    protected IndexLinkedList getItemList(){
        return list;
    }

    protected Object getValue(IndexItem item){
        Object result=null;
        if(item!=null){
            try{
                DataItem data=item.getValueDataItem();
                result=dataManager.readItem(valueMarshaller,data);
            }catch(IOException e){
                log.error("Failed to get value for "+item,e);
                throw new RuntimeStoreException(e);
            }
        }
        return result;
    }

    protected IndexItem write(Object key,Object value){
        IndexItem index=null;
        try{
            if(key!=null){
                index=indexManager.createNewIndex();
                DataItem data=dataManager.storeItem(keyMarshaller,key);
                index.setKeyData(data);
            }
            if(value!=null){
                DataItem data=dataManager.storeItem(valueMarshaller,value);
                index.setValueData(data);
            }
            IndexItem last=list.isEmpty()?null:(IndexItem) list.getLast();
            last=last==null?root:last;
            long prev=last.getOffset();
            index.setPreviousItem(prev);
            indexManager.updateIndex(index);
            last.setNextItem(index.getOffset());
            indexManager.updateIndex(last);
        }catch(IOException e){
            log.error("Failed to write "+key+" , "+value,e);
            throw new RuntimeStoreException(e);
        }
        return index;
    }
}