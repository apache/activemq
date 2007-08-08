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

import java.util.Iterator;
import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexLinkedList;

/**
 * Values collection iterator for the MapContainer
 * 
 * @version $Revision: 1.2 $
 */
public class ContainerValueCollectionIterator implements Iterator {

    protected BaseContainerImpl container;
    protected IndexLinkedList list;
    protected IndexItem nextItem;
    protected IndexItem currentItem;

    ContainerValueCollectionIterator(BaseContainerImpl container, IndexLinkedList list, IndexItem start) {
        this.container = container;
        this.list = list;
        this.currentItem = start;
        this.nextItem = list.getNextEntry((IndexItem)list.refreshEntry(start));
    }

    public boolean hasNext() {
        return nextItem != null;
    }

    public Object next() {
        synchronized (container) {
            nextItem = (IndexItem)list.refreshEntry(nextItem);
            currentItem = nextItem;
            Object result = container.getValue(nextItem);
            nextItem = list.getNextEntry(nextItem);
            return result;
        }
    }

    public void remove() {
        synchronized (container) {
            if (currentItem != null) {
                currentItem = (IndexItem)list.refreshEntry(currentItem);
                container.remove(currentItem);
            }
        }
    }
}
