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

import java.util.ListIterator;

/** 
* @version $Revision: 1.2 $
*/
public class ContainerListIterator extends ContainerValueCollectionIterator implements ListIterator{
    
   

    protected ContainerListIterator(ListContainerImpl container,IndexLinkedList list,IndexItem start){
       super(container,list,start);
    }

   
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasPrevious()
     */
    public boolean hasPrevious(){
        return list.getPrevEntry(currentItem) != null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previous()
     */
    public Object previous(){
        currentItem = list.getPrevEntry(currentItem);
        return currentItem != null ? container.getValue(currentItem) : null;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#nextIndex()
     */
    public int nextIndex(){
        int result = -1;
        if (currentItem != null){
            IndexItem next = list.getNextEntry(currentItem);
            if (next != null){
                result = container.getInternalList().indexOf(next);
            }
        }
        
        
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previousIndex()
     */
    public int previousIndex(){
        int result = -1;
        if (currentItem != null){
            IndexItem prev = list.getPrevEntry(currentItem);
            if (prev != null){
                result = container.getInternalList().indexOf(prev);
            }
        }
        
        
        return result;
    }

    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#set(E)
     */
    public void set(Object o){
        IndexItem item=((ListContainerImpl) container).internalSet(previousIndex()+1,o);
        currentItem=item;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#add(E)
     */
    public void add(Object o){
        IndexItem item=((ListContainerImpl) container).internalSet(previousIndex()+1,o);
        currentItem=item;
    }
}
