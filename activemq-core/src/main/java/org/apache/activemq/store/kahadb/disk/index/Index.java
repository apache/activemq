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
package org.apache.activemq.store.kahadb.disk.index;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.activemq.store.kahadb.disk.page.Transaction;
import org.apache.activemq.store.kahadb.disk.util.Marshaller;

/**
 * Simpler than a Map
 * 
 * 
 */
public interface Index<Key,Value> {
    
    /**
     * Set the marshaller for key objects
     * 
     * @param marshaller
     */
    void setKeyMarshaller(Marshaller<Key> marshaller);
    
    /**
     * Set the marshaller for key objects
     * 
     * @param marshaller
     */
    void setValueMarshaller(Marshaller<Value> marshaller);

    /**
     * load indexes
     */
    void load(Transaction tx) throws IOException;

    /**
     * unload indexes
     * 
     * @throws IOException
     */
    void unload(Transaction tx) throws IOException;

    /**
     * clear the index
     * 
     * @throws IOException
     * 
     */
    void clear(Transaction tx) throws IOException;

    /**
     * @param key
     * @return true if it contains the key
     * @throws IOException
     */
    boolean containsKey(Transaction tx, Key key) throws IOException;

    /**
     * remove the index key
     * 
     * @param key
     * @return StoreEntry removed
     * @throws IOException
     */
    Value remove(Transaction tx, Key key) throws IOException;

    /**
     * store the key, item
     * 
     * @param key
     * @param entry
     * @throws IOException
     */
    Value put(Transaction tx, Key key, Value entry) throws IOException;

    /**
     * @param key
     * @return the entry
     * @throws IOException
     */
    Value get(Transaction tx, Key key) throws IOException;

    /**
     * @return true if the index is transient
     */
    boolean isTransient();
    
    /**
     * @param tx
     * @return
     * @throws IOException
     * @trhows UnsupportedOperationException 
     *         if the index does not support fast iteration of the elements.
     */
    Iterator<Map.Entry<Key,Value>> iterator(final Transaction tx) throws IOException, UnsupportedOperationException;
    
}
