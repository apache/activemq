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

import java.util.HashMap;
import java.util.Map;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreEntry;

/**
 * Index implementation using a HashMap
 * 
 * @version $Revision: 1.2 $
 */
public class VMIndex implements Index{

    private Map<Object,StoreEntry> map=new HashMap<Object,StoreEntry>();

    /**
     * 
     * @see org.apache.activemq.kaha.impl.index.Index#clear()
     */
    public void clear(){
        map.clear();
    }

    /**
     * @param key
     * @return true if the index contains the key
     * @see org.apache.activemq.kaha.impl.index.Index#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key){
        return map.containsKey(key);
    }

    /**
     * @param key
     * @see org.apache.activemq.kaha.impl.index.Index#removeKey(java.lang.Object)
     */
    public StoreEntry remove(Object key){
       return  map.remove(key);
    }

    /**
     * @param key
     * @param entry
     * @see org.apache.activemq.kaha.impl.index.Index#store(java.lang.Object,
     *      org.apache.activemq.kaha.impl.index.IndexItem)
     */
    public void store(Object key,StoreEntry entry){
        map.put(key,entry);
    }

    /**
     * @param key
     * @return the entry
     */
    public StoreEntry get(Object key){
        return map.get(key);
    }

    /**
     * @return true if the index is transient
     */
    public boolean isTransient(){
        return true;
    }

    /**
     * load indexes
     */
    public void load(){
    }

    /**
     * unload indexes
     */
    public void unload(){
        map.clear();
    }
    
   
    public void setKeyMarshaller(Marshaller marshaller){
    }
}
