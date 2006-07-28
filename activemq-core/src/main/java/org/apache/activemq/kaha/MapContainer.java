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
package org.apache.activemq.kaha;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 *Represents a container of persistent objects in the store
 *Acts as a map, but values can be retrieved in insertion order
 * 
 * @version $Revision: 1.2 $
 */
public interface MapContainer  extends   Map{
    
    
    /**
     * The container is created or retrieved in 
     * an unloaded state.
     * load populates the container will all the indexes used etc
     * and should be called before any operations on the container
     */
    public void load();
    
    /**
     * unload indexes from the container
     *
     */
    public void unload();
    
    /**
     * @return true if the indexes are loaded
     */
    public boolean isLoaded();
    
    /**
     * For homogenous containers can set a custom marshaller for loading keys
     * The default uses Object serialization
     * @param keyMarshaller
     */
    public void setKeyMarshaller(Marshaller keyMarshaller);
    
    /**
     * For homogenous containers can set a custom marshaller for loading values
     * The default uses Object serialization
     * @param valueMarshaller 
   
     */
    public void setValueMarshaller(Marshaller valueMarshaller);
    /**
     * @return the id the MapContainer was create with
     */
    public Object getId();

    /**
     * @return the number of values in the container
     */
    public int size();

    /**
     * @return true if there are no values stored in the container
     */
    public boolean isEmpty();

    /**
     * @param key 
     * @return true if the container contains the key
     */
    public boolean containsKey(Object key);

    /**
     * Get the value associated with the key
     * @param key 
     * @return the value associated with the key from the store
     */
    public Object get(Object key);

    
    /**
     * @param o 
     * @return true if the MapContainer contains the value o
     */
    public boolean containsValue(Object o);

    /**
     * Add add entries in the supplied Map
     * @param map
     */
    public void putAll(Map map);

    /**
     * @return a Set of all the keys
     */
    public Set keySet();

    /**
     * @return a collection of all the values - the values will be lazily pulled out of the
     * store if iterated etc.
     */
    public Collection values();

    /**
     * @return a Set of all the Map.Entry instances - the values will be lazily pulled out of the
     * store if iterated etc.
     */
    public Set entrySet();

   
    /**
     * Add an entry
     * @param key
     * @param value
     * @return the old value for the key
     */
    public Object put(Object key,Object value);


    /**
     * remove an entry associated with the key
     * @param key 
     * @return the old value assocaited with the key or null
     */
    public Object remove(Object key);

    /**
     * empty the container
     */
    public void clear();
}