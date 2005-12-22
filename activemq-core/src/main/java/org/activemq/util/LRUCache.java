/**
 *
 * Copyright 2004 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.activemq.util;

import java.util.LinkedHashMap;
import java.util.Map;
/**
 * A Simple LRU Cache
 * 
 * @version $Revision$
 */

public class LRUCache extends LinkedHashMap{
    private static final long serialVersionUID=-342098639681884413L;
    protected int maxCacheSize=10000;

    
    /**
     * Constructs LRU Cache
     * 
     */
    public LRUCache(){
        super(1000,0.75f,true);
    }

    

    /**
     * @return Returns the maxCacheSize.
     */
    public int getMaxCacheSize(){
        return maxCacheSize;
    }

    /**
     * @param maxCacheSize
     *            The maxCacheSize to set.
     */
    public void setMaxCacheSize(int maxCacheSize){
        this.maxCacheSize=maxCacheSize;
    }
    
    protected boolean removeEldestEntry(Map.Entry entry){
        return size() > maxCacheSize;
    }
}
