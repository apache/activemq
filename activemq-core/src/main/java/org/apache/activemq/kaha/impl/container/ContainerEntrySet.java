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
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * Set of Map.Entry objects for a container
 * 
 * 
 */
public class ContainerEntrySet extends ContainerCollectionSupport implements Set {
    ContainerEntrySet(MapContainerImpl container) {
        super(container);
    }

    public boolean contains(Object o) {
        return container.entrySet().contains(o);
    }

    public Iterator iterator() {
        return new ContainerEntrySetIterator(container, buildEntrySet().iterator());
    }

    public Object[] toArray() {
        return buildEntrySet().toArray();
    }

    public Object[] toArray(Object[] a) {
        return buildEntrySet().toArray(a);
    }

    public boolean add(Object o) {
        throw new UnsupportedOperationException("Cannot add here");
    }

    public boolean remove(Object o) {
        boolean result = false;
        if (buildEntrySet().remove(o)) {
            ContainerMapEntry entry = (ContainerMapEntry)o;
            container.remove(entry.getKey());
        }
        return result;
    }

    public boolean containsAll(Collection c) {
        return buildEntrySet().containsAll(c);
    }

    public boolean addAll(Collection c) {
        throw new UnsupportedOperationException("Cannot add here");
    }

    public boolean retainAll(Collection c) {
        List<Object> tmpList = new ArrayList<Object>();
        for (Iterator i = c.iterator(); i.hasNext();) {
            Object o = i.next();
            if (!contains(o)) {
                tmpList.add(o);
            }
        }
        boolean result = false;
        for (Iterator<Object> i = tmpList.iterator(); i.hasNext();) {
            result |= remove(i.next());
        }
        return result;
    }

    public boolean removeAll(Collection c) {
        boolean result = true;
        for (Iterator i = c.iterator(); i.hasNext();) {
            if (!remove(i.next())) {
                result = false;
            }
        }
        return result;
    }

    public void clear() {
        container.clear();
    }

    protected Set<ContainerMapEntry> buildEntrySet() {
        Set<ContainerMapEntry> set = new HashSet<ContainerMapEntry>();
        for (Iterator i = container.keySet().iterator(); i.hasNext();) {
            ContainerMapEntry entry = new ContainerMapEntry(container, i.next());
            set.add(entry);
        }
        return set;
    }
}
