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
import java.util.HashMap;
import java.util.Map;

import org.apache.activemq.kaha.IndexMBean;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.StoreEntry;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Index implementation using a HashMap
 * 
 * @version $Revision: 1.2 $
 */
public class VMIndex implements Index, IndexMBean {
    private static final Log LOG = LogFactory.getLog(VMIndex.class);
    private IndexManager indexManager;
    private Map<Object, StoreEntry> map = new HashMap<Object, StoreEntry>();

    public VMIndex(IndexManager manager) {
        this.indexManager = manager;
    }

    /**
     * 
     * @see org.apache.activemq.kaha.impl.index.Index#clear()
     */
    public void clear() {
        map.clear();
    }

    /**
     * @param key
     * @return true if the index contains the key
     * @see org.apache.activemq.kaha.impl.index.Index#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key) {
        return map.containsKey(key);
    }

    /**
     * @param key
     * @return store entry
     * @see org.apache.activemq.kaha.impl.index.Index#removeKey(java.lang.Object)
     */
    public StoreEntry remove(Object key) {
        StoreEntry result = map.remove(key);
        if (result != null) {
            try {
                result = indexManager.refreshIndex((IndexItem)result);
            } catch (IOException e) {
                LOG.error("Failed to refresh entry", e);
                throw new RuntimeException("Failed to refresh entry");
            }
        }
        return result;
    }

    /**
     * @param key
     * @param entry
     * @see org.apache.activemq.kaha.impl.index.Index#store(java.lang.Object,
     *      org.apache.activemq.kaha.impl.index.IndexItem)
     */
    public void store(Object key, StoreEntry entry) {
        map.put(key, entry);
    }

    /**
     * @param key
     * @return the entry
     */
    public StoreEntry get(Object key) {
        StoreEntry result = map.get(key);
        if (result != null) {
            try {
                result = indexManager.refreshIndex((IndexItem)result);
            } catch (IOException e) {
                LOG.error("Failed to refresh entry", e);
                throw new RuntimeException("Failed to refresh entry");
            }
        }
        return result;
    }

    /**
     * @return true if the index is transient
     */
    public boolean isTransient() {
        return true;
    }

    /**
     * load indexes
     */
    public void load() {
    }

    /**
     * unload indexes
     */
    public void unload() {
        map.clear();
    }

    public void setKeyMarshaller(Marshaller marshaller) {
    }
    
    public int getSize() {
        return map.size();
    }
}
