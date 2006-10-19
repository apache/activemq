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

package org.apache.activemq.kaha.impl.index;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * A Set of keys for the RangeLimitedMap
 * 
 * @version $Revision: 1.2 $
 */
public class RangeLimitedMapKeySet implements Set{

    private RangeLimitedMap map;

    RangeLimitedMapKeySet(RangeLimitedMap map){
        this.map=map;
    }

    public boolean add(Object o){
        throw new UnsupportedOperationException("Cannot add here");
    }

    public boolean addAll(Collection c){
        throw new UnsupportedOperationException("Cannot add here");
    }

    public void clear(){
        map.clear();
    }

    public boolean contains(Object o){
        return map.containsKey(o);
    }

    public boolean containsAll(Collection c){
        boolean result=true;
        for(Iterator i=c.iterator();i.hasNext()&&result;){
            result&=contains(i.next());
        }
        return result;
    }

    public boolean isEmpty(){
        return map.isEmpty();
    }

    public Iterator iterator(){
        return new RangeLimitedMapKeySetIterator(map);
    }

    public boolean remove(Object o){
        return map.remove(o)!=null;
    }

    public boolean removeAll(Collection c){
        boolean result=true;
        for(Iterator i=c.iterator();i.hasNext();){
            result&=remove(i.next());
        }
        return result;
    }

    public boolean retainAll(Collection c){
        List tmpList=new ArrayList();
        for(Iterator i=c.iterator();i.hasNext();){
            Object o=i.next();
            if(!contains(o)){
                tmpList.add(o);
            }
        }
        for(Iterator i=tmpList.iterator();i.hasNext();){
            remove(i.next());
        }
        return !tmpList.isEmpty();
    }

    public int size(){
        return map.size();
    }

    public Object[] toArray(){
        Object[] result=new Object[map.size()];
        IndexItem item=map.getIndexList().getFirst();
        int count=0;
        while(item!=null){
            result[count++]=map.getKey(item);
            item=map.getNextEntry(item);
        }
        return result;
    }

    public Object[] toArray(Object[] result){
        IndexItem item=map.getIndexList().getFirst();
        int count=0;
        while(item!=null&&count<result.length){
            result[count++]=map.getKey(item);
            item=map.getNextEntry(item);
        }
        return result;
    }
}
