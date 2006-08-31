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
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.activemq.kaha.Store;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Implementation of a ListContainer
 * 
 * @version $Revision: 1.2 $
 */
public class ListContainerImpl extends BaseContainerImpl implements ListContainer{
    private static final Log log=LogFactory.getLog(ListContainerImpl.class);
    protected Marshaller marshaller=Store.ObjectMarshaller;
    protected LinkedList cacheList=new LinkedList();
    protected int offset=0;
    protected int maximumCacheSize=100;
    protected IndexItem lastCached;

    protected ListContainerImpl(ContainerId id,IndexItem root,IndexManager rootIndexManager,IndexManager indexManager,
                    DataManager dataManager) throws IOException{
        super(id,root,rootIndexManager,indexManager,dataManager);
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#load()
     */
    public void load(){
        checkClosed();
        if(!loaded){
            synchronized(mutex){
                if(!loaded){
                    loaded=true;
                    init();
                    try{
                        long nextItem=root.getNextItem();
                        while(nextItem!=Item.POSITION_NOT_SET){
                            IndexItem item=indexManager.getIndex(nextItem);
                            indexList.add(item);
                            itemAdded(item,indexList.size()-1,getValue(item));
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
     * @see org.apache.activemq.kaha.ListContainer#unload()
     */
    public void unload(){
        checkClosed();
        if(loaded){
            loaded=false;
            indexList.clear();
            clearCache();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#setKeyMarshaller(org.apache.activemq.kaha.Marshaller)
     */
    public void setMarshaller(Marshaller marshaller){
        checkClosed();
        this.marshaller=marshaller;
    }

    public boolean equals(Object obj){
        load();
        boolean result=false;
        if(obj!=null&&obj instanceof List){
            List other=(List) obj;
            synchronized(mutex){
                result=other.size()==size();
                if(result){
                    for(int i=0;i<indexList.size();i++){
                        Object o1=other.get(i);
                        Object o2=get(i);
                        result=o1==o2||(o1!=null&&o2!=null&&o1.equals(o2));
                        if(!result){
                            break;
                        }
                    }
                }
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#size()
     */
    public int size(){
        load();
        return indexList.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#addFirst(java.lang.Object)
     */
    public void addFirst(Object o){
        load();
        IndexItem item=writeFirst(o);
        synchronized(mutex){
            indexList.addFirst(item);
            itemAdded(item,0,o);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#addLast(java.lang.Object)
     */
    public void addLast(Object o){
        load();
        IndexItem item=writeLast(o);
        synchronized(mutex){
            indexList.addLast(item);
            itemAdded(item,indexList.size()-1,o);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#removeFirst()
     */
    public Object removeFirst(){
        load();
        Object result=null;
        synchronized(mutex){
            IndexItem item=(IndexItem) indexList.getFirst();
            if(item!=null){
                itemRemoved(0);
                result=getValue(item);
                int index=indexList.indexOf(item);
                IndexItem prev=index>0?(IndexItem) indexList.get(index-1):root;
                IndexItem next=index<(indexList.size()-1)?(IndexItem) indexList.get(index+1):null;
                indexList.removeFirst();
                delete(item,prev,next);
                item=null;
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#removeLast()
     */
    public Object removeLast(){
        load();
        Object result=null;
        synchronized(mutex){
            IndexItem last=indexList.getLast();
            if(last!=null){
                itemRemoved(indexList.size()-1);
                result=getValue(last);
                IndexItem prev=indexList.getPrevEntry(last);
                IndexItem next=null;
                indexList.removeLast();
                delete(last,prev,next);
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#isEmpty()
     */
    public boolean isEmpty(){
        load();
        return indexList.isEmpty();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#contains(java.lang.Object)
     */
    public boolean contains(Object o){
        load();
        boolean result=false;
        if(o!=null){
            synchronized(mutex){
                IndexItem next=indexList.getFirst();
                while(next!=null){
                    Object value=getValue(next);
                    if(value!=null&&value.equals(o)){
                        result=true;
                        break;
                    }
                    next=indexList.getNextEntry(next);
                }
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#iterator()
     */
    public Iterator iterator(){
        load();
        return listIterator();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#toArray()
     */
    public Object[] toArray(){
        load();
        List tmp=new ArrayList(indexList.size());
        synchronized(mutex){
            IndexItem next=indexList.getFirst();
            while(next!=null){
                Object value=getValue(next);
                tmp.add(value);
                next=indexList.getNextEntry(next);
            }
        }
        return tmp.toArray();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#toArray(T[])
     */
    public Object[] toArray(Object[] a){
        load();
        List tmp=new ArrayList(indexList.size());
        synchronized(mutex){
            IndexItem next=indexList.getFirst();
            while(next!=null){
                Object value=getValue(next);
                tmp.add(value);
                next=indexList.getNextEntry(next);
            }
        }
        return tmp.toArray(a);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#add(E)
     */
    public boolean add(Object o){
        load();
        addLast(o);
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#remove(java.lang.Object)
     */
    public boolean remove(Object o){
        load();
        boolean result=false;
        synchronized(mutex){
            int pos=0;
            IndexItem next=indexList.getFirst();
            while(next!=null){
                Object value=getValue(next);
                if(value!=null&&value.equals(o)){
                    remove(next);
                    itemRemoved(pos);
                    result=true;
                    break;
                }
                next=indexList.getNextEntry(next);
                pos++;
            }
        }
        return result;
    }

    protected void remove(IndexItem item){
        synchronized(mutex){
            IndexItem prev=indexList.getPrevEntry(item);
            IndexItem next=indexList.getNextEntry(item);
            indexList.remove(item);
            delete(item,prev,next);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#containsAll(java.util.Collection)
     */
    public boolean containsAll(Collection c){
        load();
        boolean result=false;
        synchronized(mutex){
            for(Iterator i=c.iterator();i.hasNext();){
                Object obj=i.next();
                if(!(result=contains(obj))){
                    result=false;
                    break;
                }
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#addAll(java.util.Collection)
     */
    public boolean addAll(Collection c){
        load();
        boolean result=false;
        for(Iterator i=c.iterator();i.hasNext();){
            add(i.next());
        }
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#addAll(int, java.util.Collection)
     */
    public boolean addAll(int index,Collection c){
        load();
        boolean result=false;
        ListIterator e1=listIterator(index);
        Iterator e2=c.iterator();
        while(e2.hasNext()){
            e1.add(e2.next());
            result=true;
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#removeAll(java.util.Collection)
     */
    public boolean removeAll(Collection c){
        load();
        boolean result=true;
        for(Iterator i=c.iterator();i.hasNext();){
            Object obj=i.next();
            result&=remove(obj);
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#retainAll(java.util.Collection)
     */
    public boolean retainAll(Collection c){
        load();
        List tmpList=new ArrayList();
        synchronized(mutex){
            IndexItem next=indexList.getFirst();
            while(next!=null){
                Object o=getValue(next);
                if(!c.contains(o)){
                    tmpList.add(o);
                }
                next=indexList.getNextEntry(next);
            }
        }
        for(Iterator i=tmpList.iterator();i.hasNext();){
            remove(i.next());
        }
        return !tmpList.isEmpty();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#clear()
     */
    public void clear(){
        checkClosed();
        synchronized(mutex){
            super.clear();
            doClear();
            clearCache();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#get(int)
     */
    public Object get(int index){
        load();
        return getCachedItem(index);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#set(int, E)
     */
    public Object set(int index,Object element){
        load();
        Object result=null;
        synchronized(mutex){
            IndexItem replace=indexList.isEmpty()?null:(IndexItem) indexList.get(index);
            IndexItem prev=(indexList.isEmpty()||(index-1)<0)?null:(IndexItem) indexList.get(index-1);
            IndexItem next=(indexList.isEmpty()||(index+1)>=size())?null:(IndexItem) indexList.get(index+1);
            result=getValue(replace);
            indexList.remove(index);
            delete(replace,prev,next);
            itemRemoved(index);
            add(index,element);
        }
        return result;
    }

    protected IndexItem internalSet(int index,Object element){
        synchronized(mutex){
            IndexItem replace=indexList.isEmpty()?null:(IndexItem) indexList.get(index);
            IndexItem prev=(indexList.isEmpty()||(index-1)<0)?null:(IndexItem) indexList.get(index-1);
            IndexItem next=(indexList.isEmpty()||(index+1)>=size())?null:(IndexItem) indexList.get(index+1);
            indexList.remove(index);
            delete(replace,prev,next);
            itemRemoved(index);
            return internalAdd(index,element);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#add(int, E)
     */
    public void add(int index,Object element){
        load();
        synchronized(mutex){
            IndexItem item=insert(index,element);
            indexList.add(index,item);
            itemAdded(item,index,element);
        }
    }

    protected IndexItem internalAdd(int index,Object element){
        synchronized(mutex){
            IndexItem item=insert(index,element);
            indexList.add(index,item);
            itemAdded(item,index,element);
            return item;
        }
    }

    protected IndexItem internalGet(int index){
        synchronized(mutex){
            if(index>=0&&index<indexList.size()){
                return indexList.get(index);
            }
        }
        return null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#doRemove(int)
     */
    public boolean doRemove(int index){
        load();
        boolean result=false;
        synchronized(mutex){
            IndexItem item=indexList.get(index);
            if(item!=null){
                result=true;
                IndexItem prev=indexList.getPrevEntry(item);
                prev=prev!=null?prev:root;
                IndexItem next=indexList.getNextEntry(prev);
                indexList.remove(index);
                itemRemoved(index);
                delete(item,prev,next);
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#remove(int)
     */
    public Object remove(int index){
        load();
        Object result=null;
        synchronized(mutex){
            IndexItem item=indexList.get(index);
            if(item!=null){
                itemRemoved(index);
                result=getValue(item);
                IndexItem prev=indexList.getPrevEntry(item);
                prev=prev!=null?prev:root;
                IndexItem next=indexList.getNextEntry(item);
                indexList.remove(index);
                delete(item,prev,next);
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#indexOf(java.lang.Object)
     */
    public int indexOf(Object o){
        load();
        int result=-1;
        if(o!=null){
            synchronized(mutex){
                int count=0;
                IndexItem next=indexList.getFirst();
                while(next!=null){
                    Object value=getValue(next);
                    if(value!=null&&value.equals(o)){
                        result=count;
                        break;
                    }
                    count++;
                    next=indexList.getNextEntry(next);
                }
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#lastIndexOf(java.lang.Object)
     */
    public int lastIndexOf(Object o){
        load();
        int result=-1;
        if(o!=null){
            synchronized(mutex){
                int count=indexList.size()-1;
                IndexItem next=indexList.getLast();
                while(next!=null){
                    Object value=getValue(next);
                    if(value!=null&&value.equals(o)){
                        result=count;
                        break;
                    }
                    count--;
                    next=indexList.getPrevEntry(next);
                }
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#listIterator()
     */
    public ListIterator listIterator(){
        load();
        return new CachedContainerListIterator(this,0);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#listIterator(int)
     */
    public ListIterator listIterator(int index){
        load();
        return new CachedContainerListIterator(this,index);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#subList(int, int)
     */
    public List subList(int fromIndex,int toIndex){
        load();
        List result=new ArrayList();
        int count=fromIndex;
        IndexItem next=indexList.get(fromIndex);
        while(next!=null&&count++<toIndex){
            result.add(getValue(next));
            next=indexList.getNextEntry(next);
        }
        return result;
    }

    protected IndexItem writeLast(Object value){
        IndexItem index=null;
        try{
            if(value!=null){
                DataItem data=dataManager.storeDataItem(marshaller,value);
                index=indexManager.createNewIndex();
                index.setValueData(data);
                IndexItem prev=indexList.getLast();
                prev=prev!=null?prev:root;
                IndexItem next=indexList.getNextEntry(prev);
                prev.setNextItem(index.getOffset());
                index.setPreviousItem(prev.getOffset());
                updateIndex(prev);
                if(next!=null){
                    next.setPreviousItem(index.getOffset());
                    index.setNextItem(next.getOffset());
                    updateIndex(next);
                }
                updateIndex(index);
            }
        }catch(IOException e){
            log.error("Failed to write "+value,e);
            throw new RuntimeStoreException(e);
        }
        return index;
    }

    protected IndexItem writeFirst(Object value){
        IndexItem index=null;
        try{
            if(value!=null){
                DataItem data=dataManager.storeDataItem(marshaller,value);
                index=indexManager.createNewIndex();
                index.setValueData(data);
                IndexItem prev=root;
                IndexItem next=indexList.getNextEntry(prev);
                prev.setNextItem(index.getOffset());
                index.setPreviousItem(prev.getOffset());
                updateIndex(prev);
                if(next!=null){
                    next.setPreviousItem(index.getOffset());
                    index.setNextItem(next.getOffset());
                    updateIndex(next);
                }
                updateIndex(index);
            }
        }catch(IOException e){
            log.error("Failed to write "+value,e);
            throw new RuntimeStoreException(e);
        }
        return index;
    }

    protected IndexItem insert(int insertPos,Object value){
        long pos=Item.POSITION_NOT_SET;
        IndexItem index=null;
        try{
            if(value!=null){
                DataItem data=dataManager.storeDataItem(marshaller,value);
                index=indexManager.createNewIndex();
                index.setValueData(data);
                IndexItem prev=null;
                IndexItem next=null;
                if(insertPos<=0){
                    prev=root;
                    next=indexList.getNextEntry(root);
                }else if(insertPos>=indexList.size()){
                    prev=indexList.getLast();
                    next=null;
                }else{
                    prev=indexList.get(insertPos);
                    prev=prev!=null?prev:root;
                    next=indexList.getNextEntry(prev);
                }
                prev.setNextItem(index.getOffset());
                index.setPreviousItem(prev.getOffset());
                updateIndex(prev);
                if(next!=null){
                    next.setPreviousItem(index.getOffset());
                    index.setNextItem(next.getOffset());
                    updateIndex(next);
                }
                updateIndex(index);
            }
        }catch(IOException e){
            log.error("Failed to insert "+value,e);
            throw new RuntimeStoreException(e);
        }
        return index;
    }

    protected Object getValue(IndexItem item){
        Object result=null;
        if(item!=null){
            try{
                DataItem data=item.getValueDataItem();
                result=dataManager.readItem(marshaller,data);
            }catch(IOException e){
                log.error("Failed to get value for "+item,e);
                throw new RuntimeStoreException(e);
            }
        }
        return result;
    }

    /**
     * @return a string representation of this collection.
     */
    public String toString(){
        StringBuffer result=new StringBuffer();
        result.append("[");
        Iterator i=iterator();
        boolean hasNext=i.hasNext();
        while(hasNext){
            Object o=i.next();
            result.append(String.valueOf(o));
            hasNext=i.hasNext();
            if(hasNext)
                result.append(", ");
        }
        result.append("]");
        return result.toString();
    }

    protected void itemAdded(IndexItem item,int pos,Object value){
        int cachePosition=pos-offset;
        // if pos is before the cache offset
        // we need to clear the cache
        if(pos<offset){
            clearCache();
        }
        if(cacheList.isEmpty()){
            offset=pos;
            cacheList.add(value);
            lastCached=item;
        }else if(cachePosition==cacheList.size()&&cachePosition<maximumCacheSize){
            cacheList.add(value);
            lastCached=item;
        }else if(cachePosition>=0&&cachePosition<=cacheList.size()){
            cacheList.add(cachePosition,value);
            if(cacheList.size()>maximumCacheSize){
                itemRemoved(cacheList.size()-1);
            }
        }
    }

    protected void itemRemoved(int pos){
        int lastPosition=offset+cacheList.size()-1;
        int cachePosition=pos-offset;
        if(cachePosition>=0&&cachePosition<cacheList.size()){
            if(cachePosition==lastPosition){
                if(lastCached!=null){
                    lastCached=indexList.getPrevEntry(lastCached);
                }
            }
            cacheList.remove(pos);
            if(cacheList.isEmpty()){
                clearCache();
            }
        }
    }

    protected Object getCachedItem(int pos){
        int cachePosition=pos-offset;
        Object result=null;
        if(cachePosition>=0&&cachePosition<cacheList.size()){
            result=cacheList.get(cachePosition);
        }
        if(result==null){
            if(cachePosition==cacheList.size()&&lastCached!=null){
                IndexItem item=indexList.getNextEntry(lastCached);
                if(item!=null){
                    result=getValue(item);
                    cacheList.add(result);
                    lastCached=item;
                    if(cacheList.size()>maximumCacheSize){
                        itemRemoved(0);
                    }
                }
            }else{
                IndexItem item=indexList.get(pos);
                if(item!=null){
                    result=getValue(item);
                    if(result!=null){
                        // outside the cache window - so clear
                        if(!cacheList.isEmpty()){
                            clearCache();
                        }
                        offset=pos;
                        cacheList.add(result);
                        lastCached=item;
                    }
                }
            }
        }
        return result;
    }

    /**
     * clear any cached values
     */
    public void clearCache(){
        cacheList.clear();
        offset=0;
        lastCached=null;
    }

    /**
     * @return the cacheList
     */
    public LinkedList getCacheList(){
        return cacheList;
    }

    /**
     * @param cacheList the cacheList to set
     */
    public void setCacheList(LinkedList cacheList){
        this.cacheList=cacheList;
    }

    /**
     * @return the lastCached
     */
    public IndexItem getLastCached(){
        return lastCached;
    }

    /**
     * @param lastCached the lastCached to set
     */
    public void setLastCached(IndexItem lastCached){
        this.lastCached=lastCached;
    }

    /**
     * @return the maximumCacheSize
     */
    public int getMaximumCacheSize(){
        return maximumCacheSize;
    }

    /**
     * @param maximumCacheSize the maximumCacheSize to set
     */
    public void setMaximumCacheSize(int maximumCacheSize){
        this.maximumCacheSize=maximumCacheSize;
    }

    /**
     * @return the offset
     */
    public int getOffset(){
        return offset;
    }

    /**
     * @param offset the offset to set
     */
    public void setOffset(int offset){
        this.offset=offset;
    }
    
    
}
