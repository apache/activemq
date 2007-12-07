/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.kaha.impl.index;

import org.apache.activemq.kaha.StoreEntry;

/**
 * Inteface to LinkedList of Indexes
 * 
 * @version $Revision$
 */
public interface IndexLinkedList {
    
    /**
     * Set the new Root
     * @param newRoot
     */
    void setRoot(IndexItem newRoot);

    /**
     * @return the root used by the List
     */
    IndexItem getRoot();

    /**
     * Returns the first element in this list.
     * 
     * @return the first element in this list.
     */
    IndexItem getFirst();

    /**
     * Returns the last element in this list.
     * 
     * @return the last element in this list.
     */
    IndexItem getLast();

    /**
     * Removes and returns the first element from this list.
     * 
     * @return the first element from this list.
     */
    StoreEntry removeFirst();

    /**
     * Removes and returns the last element from this list.
     * 
     * @return the last element from this list.
     */
    Object removeLast();

    /**
     * Inserts the given element at the beginning of this list.
     * 
     * @param item
     */
    void addFirst(IndexItem item);

    /**
     * Appends the given element to the end of this list. (Identical in function
     * to the <tt>add</tt> method; included only for consistency.)
     * 
     * @param item
     */
    void addLast(IndexItem item);

    /**
     * Returns the number of elements in this list.
     * 
     * @return the number of elements in this list.
     */
    int size();

    /**
     * is the list empty?
     * 
     * @return true if there are no elements in the list
     */
    boolean isEmpty();

    /**
     * Appends the specified element to the end of this list.
     * 
     * @param item
     * 
     * @return <tt>true</tt> (as per the general contract of
     *         <tt>Collection.add</tt>).
     */
    boolean add(IndexItem item);

    /**
     * Removes all of the elements from this list.
     */
    void clear();

    // Positional Access Operations
    /**
     * Returns the element at the specified position in this list.
     * 
     * @param index index of element to return.
     * @return the element at the specified position in this list.
     * 
     * @throws IndexOutOfBoundsException if the specified index is is out of
     *                 range (<tt>index &lt; 0 || index &gt;= size()</tt>).
     */
    IndexItem get(int index);

    /**
     * Inserts the specified element at the specified position in this list.
     * Shifts the element currently at that position (if any) and any subsequent
     * elements to the right (adds one to their indices).
     * 
     * @param index index at which the specified element is to be inserted.
     * @param element element to be inserted.
     * 
     * @throws IndexOutOfBoundsException if the specified index is out of range (<tt>index &lt; 0 || index &gt; size()</tt>).
     */
    void add(int index, IndexItem element);

    /**
     * Removes the element at the specified position in this list. Shifts any
     * subsequent elements to the left (subtracts one from their indices).
     * Returns the element that was removed from the list.
     * 
     * @param index the index of the element to removed.
     * @return the element previously at the specified position.
     * 
     * @throws IndexOutOfBoundsException if the specified index is out of range (<tt>index &lt; 0 || index &gt;= size()</tt>).
     */
    Object remove(int index);

    // Search Operations
    /**
     * Returns the index in this list of the first occurrence of the specified
     * element, or -1 if the List does not contain this element. More formally,
     * returns the lowest index i such that
     * <tt>(o==null ? get(i)==null : o.equals(get(i)))</tt>, or -1 if there
     * is no such index.
     * 
     * @param o element to search for.
     * @return the index in this list of the first occurrence of the specified
     *         element, or -1 if the list does not contain this element.
     */
    int indexOf(StoreEntry o);

    /**
     * Retrieve the next entry after this entry
     * 
     * @param entry
     * @return next entry
     */
    IndexItem getNextEntry(IndexItem entry);

    /**
     * Retrive the prev entry after this entry
     * 
     * @param entry
     * @return prev entry
     */
    IndexItem getPrevEntry(IndexItem entry);

    /**
     * remove an entry
     * 
     * @param e
     */
    void remove(IndexItem e);

    /**
     * Ensure we have the up to date entry
     * 
     * @param entry
     * @return the entry
     */
    StoreEntry getEntry(StoreEntry entry);

    /**
     * Update the indexes of a StoreEntry
     * 
     * @param current
     * @return update StoreEntry
     */
    StoreEntry refreshEntry(StoreEntry current);
}
