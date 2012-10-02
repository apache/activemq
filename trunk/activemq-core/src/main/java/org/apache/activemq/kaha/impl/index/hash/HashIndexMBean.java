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
package org.apache.activemq.kaha.impl.index.hash;

import org.apache.activemq.kaha.IndexMBean;

/**
 * MBean for HashIndex
 *
 */
public interface HashIndexMBean extends IndexMBean{
   
    /**
     * @return the keySize
     */
    public int getKeySize();

    /**
     * @param keySize the keySize to set
     */
    public void setKeySize(int keySize);

    
    /**
     * @return the page size
     */
    public int getPageSize();

        
    /**
     * @return number of bins
     */
    public int getNumberOfBins();


    /**
     * @return the enablePageCaching
     */
    public boolean isEnablePageCaching();

    
    /**
     * @return the pageCacheSize
     */
    public int getPageCacheSize();

    /**
     * @return size
     */
    public int getSize();
    
    /**
     * @return the number of active bins
     */
    public int getActiveBins();
}
