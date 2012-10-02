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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.activemq.kaha.impl.index.IndexItem;
import org.apache.activemq.kaha.impl.index.IndexLinkedList;

/**
 * Values collection for the MapContainer
 * 
 * 
 */
class ContainerValueCollection extends ContainerCollectionSupport implements Collection {

    ContainerValueCollection(MapContainerImpl container) {
        super(container);
    }

    public boolean contains(Object o) {
        return container.containsValue(o);
    }

    public Iterator iterator() {
        IndexLinkedList list = container.getItemList();
        return new ContainerValueCollectionIterator(container, list, list.getRoot());
    }

    public Object[] toArray() {
        Object[] result = null;
        IndexLinkedList list = container.getItemList();
        synchronized (list) {
            result = new Object[list.size()];
            IndexItem item = list.getFirst();
            int count = 0;
            while (item != null) {
                Object value = container.getValue(item);
                result[count++] = value;

                item = list.getNextEntry(item);
            }

        }
        return result;
    }

    public Object[] toArray(Object[] result) {
        IndexLinkedList list = container.getItemList();
        synchronized (list) {
            if (result.length <= list.size()) {
                IndexItem item = list.getFirst();
                int count = 0;
                while (item != null) {
                    Object value = container.getValue(item);
                    result[count++] = value;

                    item = list.getNextEntry(item);
                }
            }
        }
        return result;
    }

    public boolean add(Object o) {
        throw new UnsupportedOperationException("Can't add an object here");
    }

    public boolean remove(Object o) {
        return container.removeValue(o);
    }

    public boolean containsAll(Collection c) {
        boolean result = !c.isEmpty();
        for (Iterator i = c.iterator(); i.hasNext();) {
            if (!contains(i.next())) {
                result = false;
                break;
            }
        }
        return result;
    }

    public boolean addAll(Collection c) {
        throw new UnsupportedOperationException("Can't add everything here!");
    }

    public boolean removeAll(Collection c) {
        boolean result = true;
        for (Iterator i = c.iterator(); i.hasNext();) {
            Object obj = i.next();
            result &= remove(obj);
        }
        return result;
    }

    public boolean retainAll(Collection c) {
        List<Object> tmpList = new ArrayList<Object>();
        for (Iterator i = c.iterator(); i.hasNext();) {
            Object o = i.next();
            if (!contains(o)) {
                tmpList.add(o);
            }
        }
        for (Iterator<Object> i = tmpList.iterator(); i.hasNext();) {
            remove(i.next());
        }
        return !tmpList.isEmpty();
    }

    public void clear() {
        container.clear();
    }
}
