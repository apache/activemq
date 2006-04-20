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
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.ObjectMarshaller;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
/**
 * Implementation of a ListContainer
 * 
 * @version $Revision: 1.2 $
 */
final class ListContainerImpl extends BaseContainerImpl implements ListContainer{
    private static final Log log=LogFactory.getLog(ListContainerImpl.class);
    protected Marshaller marshaller=new ObjectMarshaller();

    protected ListContainerImpl(Object id,IndexItem root,IndexManager indexManager,DataManager dataManager)
                    throws IOException{
        super(id,root,indexManager,dataManager);
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
                    try{
                        long nextItem=root.getNextItem();
                        while(nextItem!=Item.POSITION_NOT_SET){
                            IndexItem item=indexManager.getIndex(nextItem);
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
     * @see org.apache.activemq.kaha.ListContainer#unload()
     */
    public void unload(){
        checkClosed();
        if(loaded){
            loaded=false;
            list.clear();
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
                    for(int i=0;i<list.size();i++){
                        Object o1=other.get(i);
                        Object o2=get(i);
                        result=o1==o2||(o1!=null&&o2!=null&&o1.equals(o2));
                        if(!result)
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
     * @see org.apache.activemq.kaha.ListContainer#size()
     */
    public int size(){
        load();
        return list.size();
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
            list.addFirst(item);
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
            list.addLast(item);
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
            IndexItem item=(IndexItem) list.getFirst();
            if(item!=null){
                result=getValue(item);
                int index=list.indexOf(item);
                IndexItem prev=index>0?(IndexItem) list.get(index-1):root;
                IndexItem next=index<(list.size()-1)?(IndexItem) list.get(index+1):null;
                list.removeFirst();
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
            IndexItem item=(IndexItem) list.getLast();
            if(item!=null){
                result=getValue(item);
                int index=list.indexOf(item);
                IndexItem prev=index>0?(IndexItem) list.get(index-1):root;
                IndexItem next=null;
                list.removeLast();
                delete(item,prev,next);
                item=null;
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
        return list.isEmpty();
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
                IndexItem next=list.getFirst();
                while(next!=null){
                    Object value=getValue(next);
                    if(value!=null&&value.equals(o)){
                        result=true;
                        break;
                    }
                    next=list.getNextEntry(next);
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
        List tmp=new ArrayList(list.size());
        synchronized(mutex){
            IndexItem next=list.getFirst();
            while(next!=null){
                Object value=getValue(next);
                tmp.add(value);
                next=list.getNextEntry(next);
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
        List tmp=new ArrayList(list.size());
        synchronized(mutex){
            IndexItem next=list.getFirst();
            while(next!=null){
                Object value=getValue(next);
                tmp.add(value);
                next=list.getNextEntry(next);
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
            IndexItem next=list.getFirst();
            while(next!=null){
                Object value=getValue(next);
                if(value!=null&&value.equals(o)){
                    remove(next);
                    result=true;
                    break;
                }
                next=list.getNextEntry(next);
            }
        }
        return result;
    }

    protected void remove(IndexItem item){
        synchronized(mutex){
            int index=list.indexOf(item);
            IndexItem prev=index>0?(IndexItem) list.get(index-1):root;
            IndexItem next=index<(list.size()-1)?(IndexItem) list.get(index+1):null;
            list.remove(index);
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
            IndexItem next=list.getFirst();
            while(next!=null){
                Object o=getValue(next);
                if(!c.contains(o)){
                    tmpList.add(o);
                }
                next=list.getNextEntry(next);
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
            list.clear();
            doClear();
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#get(int)
     */
    public Object get(int index){
        load();
        Object result=null;
        IndexItem item=(IndexItem) list.get(index);
        if(item!=null){
            result=getValue(item);
        }
        return result;
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
            IndexItem replace=list.isEmpty()?null:(IndexItem) list.get(index);
            IndexItem prev=(list.isEmpty()||(index-1)<0)?null:(IndexItem) list.get(index-1);
            IndexItem next=(list.isEmpty()||(index+1)>=size())?null:(IndexItem) list.get(index+1);
            result=getValue(replace);
            list.remove(index);
            delete(replace,prev,next);
            add(index,element);
        }
        return result;
    }

    protected IndexItem internalSet(int index,Object element){
        synchronized(mutex){
            IndexItem replace=list.isEmpty()?null:(IndexItem) list.get(index);
            IndexItem prev=(list.isEmpty()||(index-1)<0)?null:(IndexItem) list.get(index-1);
            IndexItem next=(list.isEmpty()||(index+1)>=size())?null:(IndexItem) list.get(index+1);
            list.remove(index);
            delete(replace,prev,next);
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
            list.add(index,item);
        }
    }

    protected IndexItem internalAdd(int index,Object element){
        synchronized(mutex){
            IndexItem item=insert(index,element);
            list.add(index,item);
            return item;
        }
    }

    protected IndexItem internalGet(int index){
        synchronized(mutex){
            if(index>=0&&index<list.size()){
                return list.get(index);
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
            IndexItem item=list.get(index);
            if(item!=null){
                result=true;
                IndexItem prev=list.getPrevEntry(item);
                prev=prev!=null?prev:root;
                IndexItem next=list.getNextEntry(prev);
                list.remove(index);
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
            IndexItem item=list.get(index);
            if(item!=null){
                result=getValue(item);
                IndexItem prev=list.getPrevEntry(item);
                prev=prev!=null?prev:root;
                IndexItem next=list.getNextEntry(prev);
                list.remove(index);
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
                IndexItem next=list.getFirst();
                while(next!=null){
                    Object value=getValue(next);
                    if(value!=null&&value.equals(o)){
                        result=count;
                        break;
                    }
                    count++;
                    next=list.getNextEntry(next);
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
                int count=list.size()-1;
                IndexItem next=list.getLast();
                while(next!=null){
                    Object value=getValue(next);
                    if(value!=null&&value.equals(o)){
                        result=count;
                        break;
                    }
                    count--;
                    next=list.getPrevEntry(next);
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
        return new ContainerListIterator(this,list,list.getRoot());
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#listIterator(int)
     */
    public ListIterator listIterator(int index){
        load();
        IndexItem start=list.get(index);
        if(start!=null){
            start=list.getPrevEntry(start);
        }
        if(start==null){
            start=root;
        }
        return new ContainerListIterator(this,list,start);
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
        IndexItem next=list.get(fromIndex);
        while(next!=null&&count++<toIndex){
            result.add(getValue(next));
            next=list.getNextEntry(next);
        }
        return result;
    }

    protected IndexItem writeLast(Object value){
        IndexItem index=null;
        try{
            if(value!=null){
                DataItem data=dataManager.storeItem(marshaller,value);
                index=indexManager.createNewIndex();
                index.setValueData(data);
                IndexItem prev=list.getLast();
                prev=prev!=null?prev:root;
                IndexItem next=list.getNextEntry(prev);
                prev.setNextItem(index.getOffset());
                index.setPreviousItem(prev.getOffset());
                indexManager.updateIndex(prev);
                if(next!=null){
                    next.setPreviousItem(index.getOffset());
                    index.setNextItem(next.getOffset());
                    indexManager.updateIndex(next);
                }
                indexManager.updateIndex(index);
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
                DataItem data=dataManager.storeItem(marshaller,value);
                index=indexManager.createNewIndex();
                index.setValueData(data);
                IndexItem prev=root;
                IndexItem next=list.getNextEntry(prev);
                prev.setNextItem(index.getOffset());
                index.setPreviousItem(prev.getOffset());
                indexManager.updateIndex(prev);
                if(next!=null){
                    next.setPreviousItem(index.getOffset());
                    index.setNextItem(next.getOffset());
                    indexManager.updateIndex(next);
                }
                indexManager.updateIndex(index);
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
                DataItem data=dataManager.storeItem(marshaller,value);
                index=indexManager.createNewIndex();
                index.setValueData(data);
                IndexItem prev=null;
                IndexItem next=null;
                if(insertPos<=0){
                    prev=root;
                    next=list.getNextEntry(root);
                }else if(insertPos>=list.size()){
                    prev=list.getLast();
                    next=null;
                }else{
                    prev=list.get(insertPos);
                    prev=prev!=null?prev:root;
                    next=list.getNextEntry(prev);
                }
                prev.setNextItem(index.getOffset());
                index.setPreviousItem(prev.getOffset());
                indexManager.updateIndex(prev);
                if(next!=null){
                    next.setPreviousItem(index.getOffset());
                    index.setNextItem(next.getOffset());
                    indexManager.updateIndex(next);
                }
                indexManager.updateIndex(index);
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
}
