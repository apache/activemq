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

import java.io.IOException;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.ObjectMarshaller;
import org.apache.activemq.kaha.Store;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import edu.emory.mathcs.backport.java.util.concurrent.ConcurrentHashMap;

/**
* A container of roots for other Containers
* 
* @version $Revision: 1.2 $
*/

class IndexRootContainer {
    private static final Log log=LogFactory.getLog(IndexRootContainer.class);
    protected static final Marshaller rootMarshaller = Store.ObjectMarshaller;
    protected IndexItem root;
    protected IndexManager indexManager;
    protected DataManager dataManager;
    protected Map map = new ConcurrentHashMap();
    protected LinkedList list = new LinkedList();
    
    
    IndexRootContainer(IndexItem root,IndexManager im,DataManager dfm) throws IOException{
        this.root=root;
        this.indexManager=im;
        this.dataManager=dfm;
        long nextItem=root.getNextItem();
        while(nextItem!=Item.POSITION_NOT_SET){
            IndexItem item=indexManager.getIndex(nextItem);
            DataItem data=item.getKeyDataItem();
            Object key=dataManager.readItem(rootMarshaller,data);
            map.put(key,item);
            list.add(item);
            nextItem=item.getNextItem();
            dataManager.addInterestInFile(item.getKeyFile());
        }
    }
    
    Set getKeys(){
        return map.keySet();
    }
    
    
    
    IndexItem addRoot(Object key) throws IOException{
        if (map.containsKey(key)){
            removeRoot(key);
        }
        
        DataItem data = dataManager.storeDataItem(rootMarshaller, key);
        IndexItem index = indexManager.createNewIndex();
        index.setKeyData(data);
        IndexItem newRoot = indexManager.createNewIndex();
        indexManager.updateIndex(newRoot);
        index.setValueOffset(newRoot.getOffset());
       
        IndexItem last=list.isEmpty()?null:(IndexItem) list.getLast();
        last=last==null?root:last;
        long prev=last.getOffset();
        index.setPreviousItem(prev);
        indexManager.updateIndex(index);
        last.setNextItem(index.getOffset());
        indexManager.updateIndex(last);
        map.put(key, index);
        list.add(index);
        return newRoot;
    }
    
    void removeRoot(Object key) throws IOException{
        IndexItem item = (IndexItem) map.remove(key);
        if (item != null){
            dataManager.removeInterestInFile(item.getKeyFile());
            IndexItem rootIndex = indexManager.getIndex(item.getValueOffset());
            indexManager.freeIndex(rootIndex);
            int index=list.indexOf(item);
            IndexItem prev=index>0?(IndexItem) list.get(index-1):root;
            prev=prev==null?root:prev;
            IndexItem next=index<(list.size()-1)?(IndexItem) list.get(index+1):null;
            if(next!=null){
                prev.setNextItem(next.getOffset());
                next.setPreviousItem(prev.getOffset());
                indexManager.updateIndex(next);
            }else{
                prev.setNextItem(Item.POSITION_NOT_SET);
            }
            indexManager.updateIndex(prev);
            list.remove(item);
        }
    }
    
    IndexItem getRoot(Object key) throws IOException{
        IndexItem index =  (IndexItem) map.get(key);
        if (index != null){
            return indexManager.getIndex(index.getValueOffset());
        }
        return null;
    }

    

}