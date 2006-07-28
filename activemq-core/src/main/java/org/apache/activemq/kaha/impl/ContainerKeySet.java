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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
* A Set of keys for the container
* 
* @version $Revision: 1.2 $
*/
public class ContainerKeySet extends ContainerCollectionSupport implements Set{
  
    
    ContainerKeySet(MapContainerImpl container){
        super(container);
    }
    
    
    public boolean contains(Object o){
        return container.getInternalKeySet().contains(o);
    }

    public Iterator iterator(){
        return new ContainerKeySetIterator(container,container.getInternalKeySet().iterator());
    }

    public Object[] toArray(){
        return container.getInternalKeySet().toArray();
    }

    public Object[] toArray(Object[] a){
        return container.getInternalKeySet().toArray(a);
    }

    public boolean add(Object o){
        throw new UnsupportedOperationException("Cannot add here");
    }

    public boolean remove(Object o){
       return container.remove(o) != null;
    }

    public boolean containsAll(Collection c){
       return container.getInternalKeySet().containsAll(c);
    }

    public boolean addAll(Collection c){
        throw new UnsupportedOperationException("Cannot add here");
    }

    public boolean retainAll(Collection c){
        List tmpList = new ArrayList();
        for (Iterator i = c.iterator(); i.hasNext(); ){
            Object o = i.next();
            if (!contains(o)){
                tmpList.add(o);
            }  
        }
        for(Iterator i = tmpList.iterator(); i.hasNext();){
            remove(i.next());
        }
        return !tmpList.isEmpty();
    }

    public boolean removeAll(Collection c){
        boolean result = true;
        for (Iterator i = c.iterator(); i.hasNext(); ){
            if (!remove(i.next())){
                result = false;
            }
        }
        return result;
    }

    public void clear(){
      container.clear();
    }
}
