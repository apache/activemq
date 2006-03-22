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
import java.util.LinkedList;
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
public class ListContainerImpl implements ListContainer{
    private static final Log log=LogFactory.getLog(MapContainerImpl.class);
    protected StoreImpl store;
    protected LocatableItem root;
    protected Object id;
    protected LinkedList list=new LinkedList();
    protected boolean loaded=false;
    protected Marshaller marshaller=new ObjectMarshaller();
    protected boolean closed = false;

    protected ListContainerImpl(Object id,StoreImpl rfs,LocatableItem root) throws IOException{
        this.id=id;
        this.store=rfs;
        this.root=root;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#load()
     */
    public void load(){
        checkClosed();
        if(!loaded){
            loaded=true;
            long start=root.getNextItem();
            if(start!=Item.POSITION_NOT_SET){
                try{
                    long nextItem=start;
                    while(nextItem!=Item.POSITION_NOT_SET){
                        LocatableItem item=new LocatableItem();
                        item.setOffset(nextItem);
                        store.readLocation(item);
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

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#unload()
     */
    public void unload(){
        checkClosed();
        if(loaded){
            loaded = false;
            list.clear();
        }
    }
    
    public void close(){
        unload();
        closed = true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#isLoaded()
     */
    public boolean isLoaded(){
        checkClosed();
        return loaded;
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

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#getId()
     */
    public Object getId(){
        checkClosed();
        return id;
    }
    
    public boolean equals(Object obj){
        checkLoaded();
        checkClosed();
        boolean result = false;
        if (obj != null && obj instanceof List){
            List other = (List) obj;
            synchronized(list){
            result = other.size() == size();
            if (result){
                for (int i =0; i < list.size(); i++){
                    Object o1 = other.get(i);
                    Object o2 = get(i);
                    result = o1 == o2 || (o1 != null && o2 != null && o1.equals(o2));
                    if (!result) break;
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
        checkClosed();
        checkLoaded();
        return list.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#addFirst(java.lang.Object)
     */
    public void addFirst(Object o){
        checkClosed();
        checkLoaded();
        LocatableItem item=writeFirst(o);
        synchronized(list){
            list.addFirst(item);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#addLast(java.lang.Object)
     */
    public void addLast(Object o){
        checkClosed();
        checkLoaded();
        LocatableItem item=writeLast(o);
        synchronized(list){
            list.addLast(item);
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see org.apache.activemq.kaha.ListContainer#removeFirst()
     */
    public Object removeFirst(){
        checkClosed();
        checkLoaded();
        Object result=null;
        synchronized(list){
            LocatableItem item=(LocatableItem) list.getFirst();
            if(item!=null){
                result=getValue(item);
                int index=list.indexOf(item);
                LocatableItem prev=index>0?(LocatableItem) list.get(index-1):root;
                LocatableItem next=index<(list.size()-1)?(LocatableItem) list.get(index+1):null;
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
        checkClosed();
        checkLoaded();
        Object result=null;
        synchronized(list){
            LocatableItem item=(LocatableItem) list.getLast();
            if(item!=null){
                result=getValue(item);
                int index=list.indexOf(item);
                LocatableItem prev=index>0?(LocatableItem) list.get(index-1):root;
                LocatableItem next=null;
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
        checkClosed();
        checkLoaded();
        return list.isEmpty();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#contains(java.lang.Object)
     */
    public boolean contains(Object o){
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
     * @see java.util.List#iterator()
     */
    public Iterator iterator(){
        checkClosed();
        checkLoaded();
        return listIterator();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#toArray()
     */
    public Object[] toArray(){
        checkClosed();
        checkLoaded();
        List tmp=new ArrayList(list.size());
        synchronized(list){
            for(Iterator i=list.iterator();i.hasNext();){
                LocatableItem item=(LocatableItem) i.next();
                Object value=getValue(item);
                tmp.add(value);
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
        checkClosed();
        checkLoaded();
        List tmp=new ArrayList(list.size());
        synchronized(list){
            for(Iterator i=list.iterator();i.hasNext();){
                LocatableItem item=(LocatableItem) i.next();
                Object value=getValue(item);
                tmp.add(value);
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
        checkClosed();
        checkLoaded();
        addLast(o);
        return true;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#remove(java.lang.Object)
     */
    public boolean remove(Object o){
        checkClosed();
        checkLoaded();
        boolean result=false;
        synchronized(list){
            for(Iterator i=list.iterator();i.hasNext();){
                LocatableItem item=(LocatableItem) i.next();
                Object value = getValue(item);
                if (value != null && value.equals(o)){
                    remove(item);
                    break;
                }
            }
            
        }
        return result;
    }

    protected void remove(LocatableItem item){
        synchronized(list){
            int index=list.indexOf(item);
            LocatableItem prev=index>0?(LocatableItem) list.get(index-1):root;
            LocatableItem next=index<(list.size()-1)?(LocatableItem) list.get(index+1):null;
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
        checkClosed();
        checkLoaded();
        boolean result=false;
        synchronized(list){
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
        checkClosed();
        checkLoaded();
        boolean result=false;
        for(Iterator i=c.iterator();i.hasNext();){
            add(i.next());
            result=true;
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#addAll(int, java.util.Collection)
     */
    public boolean addAll(int index,Collection c){
        checkClosed();
        checkLoaded();
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
        checkClosed();
        checkLoaded();
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
        checkClosed();
        checkLoaded();
        List tmpList=new ArrayList();
        synchronized(list){
        for(Iterator i = list.iterator(); i.hasNext();){
            LocatableItem item = (LocatableItem) i.next();
            Object o = getValue(item);
            
            if(!c.contains(o)){
                tmpList.add(o);
            }
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
        synchronized(list){
            list.clear();
            try {
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
                store.removeItem(item);
            }
            list.clear();
            }catch(IOException e){
                log.error("Failed to clear ListContainer "+getId(),e);
                throw new RuntimeStoreException(e);
            }
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#get(int)
     */
    public Object get(int index){
        checkClosed();
        checkLoaded();
        Object result=null;
        LocatableItem item=(LocatableItem) list.get(index);
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
        checkClosed();
        checkLoaded();
        Object result=null;
        synchronized(list){
            LocatableItem replace=list.isEmpty()?null:(LocatableItem) list.get(index);
            LocatableItem prev=(list.isEmpty() || (index-1) < 0)?null:(LocatableItem) list.get(index-1);
            LocatableItem next=(list.isEmpty() || (index+1) >= size())?null:(LocatableItem) list.get(index+1);
            result=getValue(replace);
            list.remove(index);
            delete(replace,prev,next);
            add(index,element);
        }
        return result;
    }
    
    protected LocatableItem internalSet(int index,Object element){
        synchronized(list){
            LocatableItem replace=list.isEmpty()?null:(LocatableItem) list.get(index);
            LocatableItem prev=(list.isEmpty() || (index-1) < 0)?null:(LocatableItem) list.get(index-1);
            LocatableItem next=(list.isEmpty() || (index+1) >= size())?null:(LocatableItem) list.get(index+1);
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
        checkClosed();
        checkLoaded();
        synchronized(list){
            LocatableItem item=insert(index,element);
            list.add(index,item);
        }
    }
    
    protected LocatableItem internalAdd(int index,Object element){
        synchronized(list){
            LocatableItem item=insert(index,element);
            list.add(index,item);
            return item;
        }
    }
    
    protected LocatableItem internalGet(int index){
        synchronized(list){
            if (index >= 0 && index < list.size()){
                return (LocatableItem) list.get(index);
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
        checkClosed();
        checkLoaded();
        boolean result=false;
        synchronized(list){
            LocatableItem item=(LocatableItem) list.get(index);
            if(item!=null){
                LocatableItem prev=index>0?(LocatableItem) list.get(index-1):root;
                LocatableItem next=index<(list.size()-1)?(LocatableItem) list.get(index+1):null;
                list.remove(index);
                delete(item,prev,next);
                result=true;
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
        checkClosed();
        checkLoaded();
        Object result=null;
        synchronized(list){
            LocatableItem item=(LocatableItem) list.get(index);
            if(item!=null){
                result=getValue(item);
                LocatableItem prev=index>0?(LocatableItem) list.get(index-1):root;
                LocatableItem next=index<(list.size()-1)?(LocatableItem) list.get(index+1):null;
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
        checkClosed();
        checkLoaded();
        int result=-1;
        if(o!=null){
            synchronized(list){
                int count=0;
                for(Iterator i=list.iterator();i.hasNext();count++){
                    LocatableItem item=(LocatableItem) i.next();
                    Object value=getValue(item);
                    if(value!=null&&value.equals(o)){
                        result=count;
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
     * @see java.util.List#lastIndexOf(java.lang.Object)
     */
    public int lastIndexOf(Object o){
        checkClosed();
        checkLoaded();
        int result=-1;
        if(o!=null){
            synchronized(list){
                int count=list.size()-1;
                for(ListIterator i=list.listIterator();i.hasPrevious();count--){
                    LocatableItem item=(LocatableItem) i.previous();
                    Object value=getValue(item);
                    if(value!=null&&value.equals(o)){
                        result=count;
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
     * @see java.util.List#listIterator()
     */
    public ListIterator listIterator(){
        checkClosed();
        checkLoaded();
        ListIterator iter = ((List) list.clone()).listIterator();
        return new ContainerListIterator(this,iter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#listIterator(int)
     */
    public ListIterator listIterator(int index){
        checkClosed();
        checkLoaded();
        List result = (List) list.clone();
        ListIterator iter = result.listIterator(index);
        return new ContainerListIterator(this,iter);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.List#subList(int, int)
     */
    public List subList(int fromIndex,int toIndex){
        checkClosed();
        checkLoaded();
        List tmp = list.subList(fromIndex, toIndex);
        LinkedList result = new LinkedList();
        for (Iterator i = tmp.iterator(); i.hasNext();){
            LocatableItem item = (LocatableItem) i.next();
            result.add(getValue(item));
        }
        return result;
    }

    protected LocatableItem writeLast(Object value){
        long pos=Item.POSITION_NOT_SET;
        LocatableItem item=null;
        try{
            LocatableItem last=list.isEmpty()?null:(LocatableItem) list.getLast();
            last=last==null?root:last;
            long prev=last.getOffset();
            long next=Item.POSITION_NOT_SET;
            item=new LocatableItem(prev,next,pos);
            next=store.storeItem(marshaller,value,item);
            if(last!=null){
                last.setNextItem(next);
                store.updateItem(last);
            }
        }catch(IOException e){
            log.error("Failed to write "+value,e);
            throw new RuntimeStoreException(e);
        }
        return item;
    }

    protected LocatableItem writeFirst(Object value){
        long pos=Item.POSITION_NOT_SET;
        LocatableItem item=null;
        try{
            LocatableItem next=list.isEmpty()?null:(LocatableItem) list.getFirst();
            LocatableItem last=root;
            long prevPos=last.getOffset();
            long nextPos=next!=null?next.getOffset():Item.POSITION_NOT_SET;
            item=new LocatableItem(prevPos,nextPos,pos);
            nextPos=store.storeItem(marshaller,value,item);
            if(last!=null){
                last.setNextItem(nextPos);
                store.updateItem(last);
            }
            if(next!=null){
                next.setPreviousItem(nextPos);
                store.updateItem(next);
            }
        }catch(IOException e){
            log.error("Failed to write "+value,e);
            throw new RuntimeStoreException(e);
        }
        return item;
    }

    protected LocatableItem insert(int insertPos,Object value){
        long pos=Item.POSITION_NOT_SET;
        LocatableItem item=null;
        try{
            int lastPos=insertPos-1;
            LocatableItem prev=(list.isEmpty() || (insertPos-1) < 0)?null:(LocatableItem) list.get(lastPos);
            LocatableItem next=(list.isEmpty() || (insertPos+1) >= size())?null:(LocatableItem) list.get(insertPos+1);
            prev=prev==null?root:prev;
            long prevPos=prev.getOffset();
            long nextPos=next!=null?next.getOffset():Item.POSITION_NOT_SET;
            item=new LocatableItem(prevPos,nextPos,pos);
            nextPos=store.storeItem(marshaller,value,item);
            if(prev!=null){
                prev.setNextItem(nextPos);
                store.updateItem(prev);
            }
            if(next!=null){
                next.setPreviousItem(nextPos);
                store.updateItem(next);
            }
        }catch(IOException e){
            log.error("Failed to insert "+value,e);
            throw new RuntimeStoreException(e);
        }
        return item;
    }

    protected Object getValue(LocatableItem item){
        Object result=null;
        if(item!=null){
            try{
                result=store.readItem(marshaller,item);
            }catch(IOException e){
                log.error("Failed to get value for "+item,e);
                throw new RuntimeStoreException(e);
            }
        }
        return result;
    }

    protected void delete(LocatableItem item,LocatableItem prev,LocatableItem next){
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
            store.removeItem(item);
        }catch(IOException e){
            log.error("Failed to delete "+item,e);
            throw new RuntimeStoreException(e);
        }
    }
    
    protected final void checkClosed(){
        if (closed){
            throw new RuntimeStoreException("The store is closed");
        }
    }
    protected final void checkLoaded(){
        if (!loaded){
            throw new RuntimeStoreException("The container is not loaded");
        }
    }
}
