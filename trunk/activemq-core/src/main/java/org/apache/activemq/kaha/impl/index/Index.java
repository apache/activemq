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
package org.apache.activemq.kaha.impl.index;

import java.io.IOException;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreEntry;

/**
 * Simplier than a Map
 * 
 * 
 */
public interface Index {

    /**
     * clear the index
     * 
     * @throws IOException
     * 
     */
    void clear() throws IOException;

    /**
     * @param key
     * @return true if it contains the key
     * @throws IOException
     */
    boolean containsKey(Object key) throws IOException;

    /**
     * remove the index key
     * 
     * @param key
     * @return StoreEntry removed
     * @throws IOException
     */
    StoreEntry remove(Object key) throws IOException;

    /**
     * store the key, item
     * 
     * @param key
     * @param entry
     * @throws IOException
     */
    void store(Object key, StoreEntry entry) throws IOException;

    /**
     * @param key
     * @return the entry
     * @throws IOException
     */
    StoreEntry get(Object key) throws IOException;

    /**
     * @return true if the index is transient
     */
    boolean isTransient();

    /**
     * load indexes
     */
    void load();

    /**
     * unload indexes
     * 
     * @throws IOException
     */
    void unload() throws IOException;

    /**
     * Set the marshaller for key objects
     * 
     * @param marshaller
     */
    void setKeyMarshaller(Marshaller marshaller);
    
    /**
     * return the size of the index
     * @return
     */
    int getSize();

    /**
     * delete all state associated with the index
     *
     * @throws IOException
     */
    void delete() throws IOException;
}
