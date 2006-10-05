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
package org.apache.activemq.kaha.impl.container;

import java.util.ListIterator;

import org.apache.activemq.kaha.StoreEntry;
import org.apache.activemq.kaha.impl.index.IndexLinkedList;

/** 
* @version $Revision$
*/
public class CachedContainerListIterator implements ListIterator{
    
    protected ListContainerImpl container;
    protected IndexLinkedList list;
    protected int pos = 0;
    protected int nextPos =0;
    protected StoreEntry currentItem;

    protected CachedContainerListIterator(ListContainerImpl container,int start){
        this.container=container;
        this.list=list;
        this.pos=start;
        this.nextPos = this.pos;
    }

    
    public boolean hasNext(){
        return nextPos >= 0 && nextPos < container.size();
    }

    public Object next(){
        pos = nextPos;
        Object result = container.getCachedItem(pos);
        nextPos++;
        return result;
    }

    public void remove(){
        container.remove(pos);
        nextPos--;
    }
   
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#hasPrevious()
     */
    public boolean hasPrevious(){
        return pos >= 0 && pos < container.size();
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previous()
     */
    public Object previous(){
        Object result = container.getCachedItem(pos);
        pos--;
        return result;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#nextIndex()
     */
    public int nextIndex(){
        return pos +1;
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#previousIndex()
     */
    public int previousIndex(){
        return pos -1;
    }

    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#set(E)
     */
    public void set(Object o){
        container.internalSet(pos,o);
    }

    /*
     * (non-Javadoc)
     * 
     * @see java.util.ListIterator#add(E)
     */
    public void add(Object o){
        container.internalAdd(previousIndex()+1,o);
      
    }
}
