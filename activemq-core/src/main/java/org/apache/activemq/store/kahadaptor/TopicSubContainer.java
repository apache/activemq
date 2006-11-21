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

package org.apache.activemq.store.kahadaptor;

import org.apache.activemq.kaha.ListContainer;
import org.apache.activemq.kaha.StoreEntry;

/**
 * Holds information for the subscriber
 * 
 * @version $Revision: 1.10 $
 */
 public class TopicSubContainer{

    private ListContainer listContainer;
    private StoreEntry batchEntry;
    
    public TopicSubContainer(ListContainer container){
        this.listContainer = container;
    }
    /**
     * @return the batchEntry
     */
     public StoreEntry getBatchEntry(){
        return this.batchEntry;
    }
    
    /**
     * @param batchEntry the batchEntry to set
     */
     public void setBatchEntry(StoreEntry batchEntry){
        this.batchEntry=batchEntry;
    }
    
    /**
     * @return the listContainer
     */
     public ListContainer getListContainer(){
        return this.listContainer;
    }
    
    /**
     * @param listContainer the listContainer to set
     */
     public void setListContainer(ListContainer container){
        this.listContainer=container;
    }
    
     public void reset() {
        batchEntry = null;
    }
   
}
