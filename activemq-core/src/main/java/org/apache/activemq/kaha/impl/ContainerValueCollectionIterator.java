/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.activemq.kaha.impl;

import java.util.Iterator;

/**
* Values collection iterator for the MapContainer
* 
* @version $Revision: 1.2 $
*/
public class ContainerValueCollectionIterator implements Iterator{
    protected BaseContainerImpl container;
    protected IndexLinkedList list;
    protected IndexItem currentItem;
    ContainerValueCollectionIterator(BaseContainerImpl container,IndexLinkedList list,IndexItem start){
        this.container = container;
        this.list = list;
        this.currentItem = start;
    }
    
    public boolean hasNext(){
        return currentItem != null && list.getNextEntry(currentItem) != null;
    }

    public Object next(){
        currentItem = list.getNextEntry(currentItem);
        return container.getValue(currentItem);
    }

    public void remove(){
       if (currentItem != null){
           container.remove(currentItem);
       }
    }
}
