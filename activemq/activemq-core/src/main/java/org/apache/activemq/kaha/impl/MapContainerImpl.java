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
public class MapContainerImpl implements MapContainer{
    private static final Log log=LogFactory.getLog(MapContainerImpl.class);
    protected StoreImpl store;
    protected LocatableItem root;
    protected Object id;
    protected Map map=new HashMap();
    protected Map valueToKeyMap=new HashMap();
    protected LinkedList list=new LinkedList();
    protected boolean loaded=false;
    protected Marshaller keyMarshaller=new ObjectMarshaller();
    protected Marshaller valueMarshaller=new ObjectMarshaller();
    protected final Object mutex=new Object();
    protected boolean closed=false;

    protected MapContainerImpl(Object id,StoreImpl si,LocatableItem root) throws IOException{
        this.id=id;
        this.store=si;
        this.root=root;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#load()
     */
    public void load(){
        checkClosed();
        if(!loaded){
            loaded=true;
            synchronized(mutex){
                try{
                    long start=root.getNextItem();
                    if(start!=Item.POSITION_NOT_SET){
                        long nextItem=start;
                        while(nextItem!=Item.POSITION_NOT_SET){
                            LocatableItem item=new LocatableItem();
                            item.setOffset(nextItem);
                            Object key=store.readItem(keyMarshaller,item);
                            map.put(key,item);
                            valueToKeyMap.put(item,key);
                            list.add(item);
                            nextItem=item.getNextItem();
                        }
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

    public void close(){
        unload();
        closed=true;
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
     * @see org.apache.activemq.kaha.MapContainer#isLoaded()
     */
    public boolean isLoaded(){
        checkClosed();
        return loaded;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#getId()
     */
    public Object getId(){
        checkClosed();
        return id;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#size()
     */
    public int size(){
        checkClosed();
        checkLoaded();
        return map.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#isEmpty()
     */
    public boolean isEmpty(){
        checkClosed();
        checkLoaded();
        return map.isEmpty();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key){
        checkClosed();
        checkLoaded();
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
        checkClosed();
        checkLoaded();
        Object result=null;
        LocatableItem item=null;
        synchronized(mutex){
            item=(LocatableItem) map.get(key);
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
        checkClosed();
        checkLoaded();
        boolean result=false;
        if(o!=null){
            synchronized(list){
                for(Iterator i=list.iterator();i.hasNext();){
                    LocatableItem item=(LocatableItem) i.next();
                    Object value=getValue(item);
                    if(value!=null&&value.equals(o)){
                        result=true;
                        break;
                    }
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
        checkClosed();
        checkLoaded();
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
        checkClosed();
        checkLoaded();
        return new ContainerKeySet(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#values()
     */
    public Collection values(){
        checkClosed();
        checkLoaded();
        return new ContainerValueCollection(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#entrySet()
     */
    public Set entrySet(){
        checkClosed();
        checkLoaded();
        return new ContainerEntrySet(this);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.MapContainer#put(java.lang.Object, java.lang.Object)
     */
    public Object put(Object key,Object value){
        checkClosed();
        checkLoaded();
        Object result=null;
        synchronized(mutex){
            if(map.containsKey(key)){
                result=remove(key);
            }
            LocatableItem item=write(key,value);
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
        checkClosed();
        checkLoaded();
        Object result=null;
        synchronized(mutex){
            LocatableItem item=(LocatableItem) map.get(key);
            if(item!=null){
                map.remove(key);
                valueToKeyMap.remove(item);
                result=getValue(item);
                int index=list.indexOf(item);
                LocatableItem prev=index>0?(LocatableItem) list.get(index-1):root;
                LocatableItem next=index<(list.size()-1)?(LocatableItem) list.get(index+1):null;
                list.remove(index);
                {
                    delete(item,prev,next);
                }
                item=null;
            }
        }
        return result;
    }

    public boolean removeValue(Object o){
        checkClosed();
        checkLoaded();
        boolean result=false;
        if(o!=null){
            synchronized(list){
                for(Iterator i=list.iterator();i.hasNext();){
                    LocatableItem item=(LocatableItem) i.next();
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
                }
            }
        }
        return result;
    }

    protected void remove(LocatableItem item){
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
                try{
                    long start=root.getNextItem();
                    if(start!=Item.POSITION_NOT_SET){
                        long nextItem=start;
                        while(nextItem!=Item.POSITION_NOT_SET){
                            LocatableItem item=new LocatableItem();
                            item.setOffset(nextItem);
                            list.add(item);
                            nextItem=item.getNextItem();
                        }
                    }
                    root.setNextItem(Item.POSITION_NOT_SET);
                    store.updateItem(root);
                    for(int i=0;i<list.size();i++){
                        LocatableItem item=(LocatableItem) list.get(i);
                        if(item.getReferenceItem()!=Item.POSITION_NOT_SET){
                            Item value=new Item();
                            value.setOffset(item.getReferenceItem());
                            store.removeItem(value);
                        }
                       
                        store.removeItem(item);
                    }
                    list.clear();
                }catch(IOException e){
                    log.error("Failed to clear MapContainer "+getId(),e);
                    throw new RuntimeStoreException(e);
                }
            }
        }
    }

    protected Set getInternalKeySet(){
        return new HashSet(map.keySet());
    }

    protected LinkedList getItemList(){
        return list;
    }

    protected Object getValue(LocatableItem item){
        Object result=null;
        if(item!=null&&item.getReferenceItem()!=Item.POSITION_NOT_SET){
            Item rec=new Item();
            rec.setOffset(item.getReferenceItem());
            try{
                result=store.readItem(valueMarshaller,rec);
            }catch(IOException e){
                log.error("Failed to get value for "+item,e);
                throw new RuntimeStoreException(e);
            }
        }
        return result;
    }

    protected LocatableItem write(Object key,Object value){
        long pos=Item.POSITION_NOT_SET;
        LocatableItem item=null;
        try{
            if(value!=null){
                Item valueItem=new Item();
                pos=store.storeItem(valueMarshaller,value,valueItem);
            }
            LocatableItem last=list.isEmpty()?null:(LocatableItem) list.getLast();
            last=last==null?root:last;
            long prev=last.getOffset();
            long next=Item.POSITION_NOT_SET;
            item=new LocatableItem(prev,next,pos);
            next=store.storeItem(keyMarshaller,key,item);
            if(last!=null){
                last.setNextItem(next);
                store.updateItem(last);
            }
        }catch(IOException e){
            e.printStackTrace();
            log.error("Failed to write "+key+" , "+value,e);
            throw new RuntimeStoreException(e);
        }
        return item;
    }

    protected void delete(LocatableItem key,LocatableItem prev,LocatableItem next){
        try{
            prev=prev==null?root:prev;
            if(next!=null){
                prev.setNextItem(next.getOffset());
                next.setPreviousItem(prev.getOffset());
                store.updateItem(next);
            }else{
                prev.setNextItem(Item.POSITION_NOT_SET);
            }
            store.updateItem(prev);
            if(key.getReferenceItem()!=Item.POSITION_NOT_SET){
                Item value=new Item();
                value.setOffset(key.getReferenceItem());
                store.removeItem(value);
            }
            store.removeItem(key);
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

    protected final void checkLoaded(){
        if(!loaded){
            throw new RuntimeStoreException("The container is not loaded");
        }
    }
}