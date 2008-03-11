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

import java.io.IOException;
import org.apache.activemq.kaha.StoreEntry;

/**
 * A linked list used by IndexItems
 * 
 * @version $Revision$
 */
public class DiskIndexLinkedList implements IndexLinkedList {
    protected IndexManager indexManager;
    protected transient IndexItem root;
    protected transient IndexItem last;
    protected transient int size;

    /**
     * Constructs an empty list.
     */
    public DiskIndexLinkedList(IndexManager im, IndexItem header) {
        this.indexManager = im;
        this.root = header;
    }

    public synchronized IndexItem getRoot() {
        return root;
    }

    public void setRoot(IndexItem e) {
        this.root = e;
    }

    /**
     * Returns the first element in this list.
     * 
     * @return the first element in this list.
     */
    public synchronized IndexItem getFirst() {
        if (size == 0) {
            return null;
        }
        return getNextEntry(root);
    }

    /**
     * Returns the last element in this list.
     * 
     * @return the last element in this list.
     */
    public synchronized IndexItem getLast() {
        if (size == 0) {
            return null;
        }
        if (last != null) {
            last.next = null;
            last.setNextItem(IndexItem.POSITION_NOT_SET);
        }
        return last;
    }

    /**
     * Removes and returns the first element from this list.
     * 
     * @return the first element from this list.
     */
    public synchronized StoreEntry removeFirst() {
        if (size == 0) {
            return null;
        }
        IndexItem result = getNextEntry(root);
        remove(result);
        return result;
    }

    /**
     * Removes and returns the last element from this list.
     * 
     * @return the last element from this list.
     */
    public synchronized Object removeLast() {
        if (size == 0) {
            return null;
        }
        StoreEntry result = last;
        remove(last);
        return result;
    }

    /**
     * Inserts the given element at the beginning of this list.
     * 
     * @param o the element to be inserted at the beginning of this list.
     */
    public synchronized void addFirst(IndexItem item) {
        if (size == 0) {
            last = item;
        }
        size++;
    }

    /**
     * Appends the given element to the end of this list. (Identical in function
     * to the <tt>add</tt> method; included only for consistency.)
     * 
     * @param o the element to be inserted at the end of this list.
     */
    public synchronized void addLast(IndexItem item) {
        size++;
        last = item;
    }

    /**
     * Returns the number of elements in this list.
     * 
     * @return the number of elements in this list.
     */
    public synchronized int size() {
        return size;
    }

    /**
     * is the list empty?
     * 
     * @return true if there are no elements in the list
     */
    public synchronized boolean isEmpty() {
        return size == 0;
    }

    /**
     * Appends the specified element to the end of this list.
     * 
     * @param o element to be appended to this list.
     * @return <tt>true</tt> (as per the general contract of
     *         <tt>Collection.add</tt>).
     */
    public synchronized boolean add(IndexItem item) {
        addLast(item);
        return true;
    }

    /**
     * Removes all of the elements from this list.
     */
    public synchronized void clear() {
        last = null;
        size = 0;
    }

    // Positional Access Operations
    /**
     * Returns the element at the specified position in this list.
     * 
     * @param index index of element to return.
     * @return the element at the specified position in this list.
     * @throws IndexOutOfBoundsException if the specified index is is out of
     *                 range (<tt>index &lt; 0 || index &gt;= size()</tt>).
     */
    public synchronized IndexItem get(int index) {
        return entry(index);
    }

    /**
     * Inserts the specified element at the specified position in this list.
     * Shifts the element currently at that position (if any) and any subsequent
     * elements to the right (adds one to their indices).
     * 
     * @param index index at which the specified element is to be inserted.
     * @param element element to be inserted.
     * @throws IndexOutOfBoundsException if the specified index is out of range (<tt>index &lt; 0 || index &gt; size()</tt>).
     */
    public synchronized void add(int index, IndexItem element) {
        if (index == size) {
            last = element;
        }
        size++;
    }

    /**
     * Removes the element at the specified position in this list. Shifts any
     * subsequent elements to the left (subtracts one from their indices).
     * Returns the element that was removed from the list.
     * 
     * @param index the index of the element to removed.
     * @return the element previously at the specified position.
     * @throws IndexOutOfBoundsException if the specified index is out of range (<tt>index &lt; 0 || index &gt;= size()</tt>).
     */
    public synchronized Object remove(int index) {
        IndexItem e = entry(index);
        remove(e);
        return e;
    }

    /**
     * Return the indexed entry.
     */
    private IndexItem entry(int index) {
        if (index < 0 || index >= size) {
            throw new IndexOutOfBoundsException("Index: " + index + ", Size: " + size);
        }
        IndexItem e = root;

        for (int i = 0; i <= index; i++) {
            e = getNextEntry(e);
        }
        if (e != null && last != null && last.equals(e)) {
            last = e;
        }
        return e;
    }

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
    public synchronized int indexOf(StoreEntry o) {
        int index = 0;
        if (size > 0) {
            for (IndexItem e = getNextEntry(root); e != null; e = getNextEntry(e)) {
                if (o.equals(e)) {
                    return index;
                }
                index++;
            }
        }
        return -1;
    }

    /**
     * Retrieve the next entry after this entry
     * 
     * @param entry
     * @return next entry
     */
    public synchronized IndexItem getNextEntry(IndexItem current) {
		IndexItem result = null;
		if (current != null) {
			current = (IndexItem) refreshEntry(current);
			if (current.getNextItem() >= 0) {
				try {
					result = indexManager.getIndex(current.getNextItem());
				} catch (IOException e) {
					throw new RuntimeException("Failed to get next index from "
							+ indexManager + " for " + current, e);
				}
			}
		}
		// essential last get's updated consistently
		if (result != null && last != null && last.equals(result)) {
			last=result;
		}
		return result;
	}

    /**
	 * Retrive the prev entry after this entry
	 * 
	 * @param entry
	 * @return prev entry
	 */
    public synchronized IndexItem getPrevEntry(IndexItem current) {
		IndexItem result = null;
		if (current != null) {
			if (current.getPreviousItem() >= 0) {
				current = (IndexItem) refreshEntry(current);
				try {
					result = indexManager.getIndex(current.getPreviousItem());
				} catch (IOException e) {
					throw new RuntimeException(
							"Failed to  get current index for " + current, e);
				}
			}
		}
		// essential root get's updated consistently
		if (result != null && root != null && root.equals(result)) {
			return null;
		}
		return result;
	}

    public synchronized StoreEntry getEntry(StoreEntry current) {
        StoreEntry result = null;
        if (current != null && current.getOffset() >= 0) {
            try {
                result = indexManager.getIndex(current.getOffset());
            } catch (IOException e) {
                throw new RuntimeException("Failed to index", e);
            }
        }
        // essential root get's updated consistently
        if (result != null && root != null && root.equals(result)) {
            return root;
        }
        return result;
    }

    /**
     * Update the indexes of a StoreEntry
     * 
     * @param current
     */
    public synchronized StoreEntry refreshEntry(StoreEntry current) {
        StoreEntry result = null;
        if (current != null && current.getOffset() >= 0) {
            try {
                result = indexManager.refreshIndex((IndexItem)current);
            } catch (IOException e) {
                throw new RuntimeException("Failed to index", e);
            }
        }
        // essential root get's updated consistently
        if (result != null && root != null && root.equals(result)) {
            return root;
        }
        return result;
    }

    public synchronized void remove(IndexItem e) {
        if (e==null || e == root || e.equals(root)) {
            return;
        }
        if (e == last || e.equals(last)) {
            if (size > 1) {
                last = (IndexItem)refreshEntry(last);
                last = getPrevEntry(last);
            } else {
                last = null;
            }
        }
        size--;
    }
}
