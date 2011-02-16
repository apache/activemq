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
package org.apache.activemq.kaha.impl.container;

import java.util.ListIterator;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexLinkedList;

/**
 * 
 */
public class ContainerListIterator extends ContainerValueCollectionIterator implements ListIterator {

    protected ContainerListIterator(ListContainerImpl container, IndexLinkedList list, IndexItem start) {
        super(container, list, start);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasPrevious()
     */
    public boolean hasPrevious() {
        synchronized (container) {
            nextItem = (IndexItem)list.refreshEntry(nextItem);
            return list.getPrevEntry(nextItem) != null;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previous()
     */
    public Object previous() {
        synchronized (container) {
            nextItem = (IndexItem)list.refreshEntry(nextItem);
            nextItem = list.getPrevEntry(nextItem);
            return nextItem != null ? container.getValue(nextItem) : null;
        }
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#nextIndex()
     */
    public int nextIndex() {
        int result = -1;
        if (nextItem != null) {
            synchronized (container) {
                nextItem = (IndexItem)list.refreshEntry(nextItem);
                StoreEntry next = list.getNextEntry(nextItem);
                if (next != null) {
                    result = container.getInternalList().indexOf(next);
                }
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previousIndex()
     */
    public int previousIndex() {
        int result = -1;
        if (nextItem != null) {
            synchronized (container) {
                nextItem = (IndexItem)list.refreshEntry(nextItem);
                StoreEntry prev = list.getPrevEntry(nextItem);
                if (prev != null) {
                    result = container.getInternalList().indexOf(prev);
                }
            }
        }
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#set(E)
     */
    public void set(Object o) {
        IndexItem item = ((ListContainerImpl)container).internalSet(previousIndex() + 1, o);
        nextItem = item;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#add(E)
     */
    public void add(Object o) {
        IndexItem item = ((ListContainerImpl)container).internalAdd(previousIndex() + 1, o);
        nextItem = item;
    }
}
