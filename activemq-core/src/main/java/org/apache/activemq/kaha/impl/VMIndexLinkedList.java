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

/**
 * A linked list used by IndexItems
 * 
 * @version $Revision: 1.2 $
 */
final class VMIndexLinkedList implements Cloneable, IndexLinkedList{
    private transient IndexItem root;
    private transient int size=0;

   
    /**
     * Constructs an empty list.
     */
    VMIndexLinkedList(IndexItem header){
        this.root = header;
        this.root.next=root.prev=root;
    }
    
    public IndexItem getRoot(){
        return root;
    }
    


    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#getFirst()
     */
    public IndexItem getFirst(){
        if(size==0)
            return null;
        return root.next;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#getLast()
     */
    public IndexItem getLast(){
        if(size==0)
            return null;
        return root.prev;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#removeFirst()
     */
    public IndexItem removeFirst(){
        if(size==0){
            return null;
        }
        IndexItem result=root.next;
        remove(root.next);
        return result;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#removeLast()
     */
    public Object removeLast(){
        if(size==0)
            return null;
        IndexItem result=root.prev;
        remove(root.prev);
        return result;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#addFirst(org.apache.activemq.kaha.impl.IndexItem)
     */
    public void addFirst(IndexItem item){
        addBefore(item,root.next);
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#addLast(org.apache.activemq.kaha.impl.IndexItem)
     */
    public void addLast(IndexItem item){
        addBefore(item,root);
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#size()
     */
    public int size(){
        return size;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#isEmpty()
     */
    public boolean isEmpty(){
        return size==0;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#add(org.apache.activemq.kaha.impl.IndexItem)
     */
    public boolean add(IndexItem item){
        addBefore(item,root);
        return true;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#clear()
     */
   public  void clear(){
        root.next=root.prev=root;
        size=0;
    }

    // Positional Access Operations
    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#get(int)
     */
    public IndexItem get(int index){
        return entry(index);
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#add(int, org.apache.activemq.kaha.impl.IndexItem)
     */
    public void add(int index,IndexItem element){
        addBefore(element,(index==size?root:entry(index)));
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#remove(int)
     */
    public Object remove(int index){
        IndexItem e=entry(index);
        remove(e);
        return e;
    }

    /**
     * Return the indexed entry.
     */
    private IndexItem entry(int index){
        if(index<0||index>=size)
            throw new IndexOutOfBoundsException("Index: "+index+", Size: "+size);
        IndexItem e=root;
        if(index<size/2){
            for(int i=0;i<=index;i++)
                e=e.next;
        }else{
            for(int i=size;i>index;i--)
                e=e.prev;
        }
        return e;
    }

    // Search Operations
    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#indexOf(org.apache.activemq.kaha.impl.IndexItem)
     */
    public int indexOf(IndexItem o){
        int index=0;
        for(IndexItem e=root.next;e!=root;e=e.next){
            if(o==e){
                return index;
            }
            index++;
        }
        return -1;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#getNextEntry(org.apache.activemq.kaha.impl.IndexItem)
     */
    public IndexItem getNextEntry(IndexItem entry){
        return entry.next != root ? entry.next : null;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#getPrevEntry(org.apache.activemq.kaha.impl.IndexItem)
     */
    public IndexItem getPrevEntry(IndexItem entry){
        return entry.prev != root ? entry.prev : null;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#addBefore(org.apache.activemq.kaha.impl.IndexItem, org.apache.activemq.kaha.impl.IndexItem)
     */
    public void addBefore(IndexItem insert,IndexItem e){
        insert.next=e;
        insert.prev=e.prev;
        insert.prev.next=insert;
        insert.next.prev=insert;
        size++;
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.kaha.impl.IndexLinkedList#remove(org.apache.activemq.kaha.impl.IndexItem)
     */
    public void remove(IndexItem e){
        if(e==root)
            return;
        e.prev.next=e.next;
        e.next.prev=e.prev;
        size--;
    }
    
    /**
     *@return clone
     */
    public Object clone(){
        IndexLinkedList clone=new VMIndexLinkedList(this.root);
        for(IndexItem e=root.next;e!=root;e=e.next)
            clone.add(e);
        return clone;
    }

    public IndexItem getEntry(IndexItem current){
        return current;
    }
}
