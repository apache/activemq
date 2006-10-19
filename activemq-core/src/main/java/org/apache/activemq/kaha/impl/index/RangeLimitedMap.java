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

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.RuntimeStoreException;
import org.apache.activemq.kaha.StoreLocation;
import org.apache.activemq.kaha.impl.data.DataManager;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * A Map of keys and IndexItems - which is limited in size
 * 
 * @version $Revision: 1.2 $
 */
public class RangeLimitedMap implements Map{

    private static final Log log=LogFactory.getLog(RangeLimitedMap.class);
    private LinkedHashMap internalMap;
    private IndexLinkedList indexList;
    private DataManager keyDataManager;
    private Marshaller keyMarshaller;
    private int maxRange=1000;
    private IndexItem rangeStart;
    private IndexItem rangeStop;

    /**
     * @param indexList
     * @param keyDataManager
     * @param keyMarshaller
     */
    public RangeLimitedMap(IndexLinkedList indexList,DataManager keyDataManager,Marshaller keyMarshaller){
        this.indexList=indexList;
        this.keyDataManager=keyDataManager;
        this.keyMarshaller=keyMarshaller;
        this.internalMap=new LinkedHashMap(){

            protected boolean removeEldestEntry(Map.Entry eldest){
                boolean result=false;
                if(size()>maxRange){
                    result=true;
                }
                if (rangeStart != null && rangeStart.equals(eldest)) {
                    rangeStart = null;
                    Iterator i = this.values().iterator();
                    if (i.hasNext()) i.next(); //eldest
                    if(i.hasNext()) {
                        rangeStart = (IndexItem)i.next();
                    }
                }
                return result;
            }
        };
    }

    /**
     * @param key
     * @return
     * @see java.util.Map#containsKey(java.lang.Object)
     */
    public boolean containsKey(Object key){
        return get(key)!=null;
    }

    /**
     * @param value
     * @return
     * @see java.util.Map#containsValue(java.lang.Object)
     */
    public boolean containsValue(Object value){
        throw new RuntimeException("Not supported");
    }

    /**
     * @return
     * @see java.util.Map#entrySet()
     */
    public Set entrySet(){
        throw new RuntimeException("Not supported");
    }

    /**
     * @param key
     * @return the value associated with the key
     * @see java.util.Map#get(java.lang.Object)
     */
    public Object get(Object key){
        Object result=internalMap.get(key);
        if(result==null){
            // page through the indexes till we get a hit
            // if rangeStart doesn't equal the the first entry
            // start from there
            IndexItem first=indexList.getFirst();
            IndexItem last=indexList.getLast();
            if(rangeStart!=null&&(first==null||first.equals(rangeStart))){
                rangeStart=null;
                rangeStop=first;
            }
            do{
                pageIntoMap(rangeStop);
                result=internalMap.get(key);
            }while(result==null||rangeStop!=null||rangeStop.equals(last));
        }
        return result;
    }

    /**
     * @return
     * @see java.util.Map#isEmpty()
     */
    public boolean isEmpty(){
        return indexList.isEmpty();
    }

    /**
     * @return
     * @see java.util.Map#keySet()
     */
    public Set keySet(){
        return new RangeLimitedMapKeySet(this);
    }

    /**
     * @param key
     * @param value
     * @return
     * @see java.util.Map#put(java.lang.Object, java.lang.Object)
     */
    public Object put(Object key,Object value){
        Object result = null;
        //we desire objects to be cached for FIFO
        if (internalMap.size() <= maxRange) {
            rangeStop = (IndexItem)value;
            internalMap.put(key,value);
        }
        return result;
    }

    /**
     * @param t
     * @see java.util.Map#putAll(java.util.Map)
     */
    public void putAll(Map t){
        for (Iterator i = t.entrySet().iterator(); i.hasNext();) {
            Map.Entry entry  = (Entry)i.next();
            put(entry.getKey(),entry.getValue());
        }
    }

    /**
     * @param key
     * @return
     * @see java.util.Map#remove(java.lang.Object)
     */
    public Object remove(Object key){
        IndexItem item = (IndexItem)internalMap.remove(key);
        if (item != null) {
            if (rangeStart != null && item.equals(rangeStart)) {
                rangeStart = null;
                Iterator i = internalMap.values().iterator();
                if (i.hasNext()) i.next(); //eldest
                if(i.hasNext()) {
                    rangeStart = (IndexItem)i.next();
                }
            }
            if (rangeStop != null && item.equals(rangeStop)) {
                rangeStop = indexList.getPrevEntry(item);
            }
        }
        return item;
    }

    /**
     * @return
     * @see java.util.Map#size()
     */
    public int size(){
        return indexList.size();
    }

    /**
     * @return
     * @see java.util.Map#values()
     */
    public Collection values(){
       throw new RuntimeException("Not supported");
    }

    /**
     * @return the maxRange
     */
    public int getMaxRange(){
        return this.maxRange;
    }

    /**
     * @param maxRange the maxRange to set
     */
    public void setMaxRange(int maxRange){
        this.maxRange=maxRange;
    }

    /**
     * fill the internalMap
     * 
     * @param start
     * @param numberOfItems
     * @return the actual number of items paged into the internalMap
     */
    protected int pageIntoMap(IndexItem start){
        int count=0;
        internalMap.clear();
        rangeStart=null;
        rangeStop=null;
        IndexItem item=start!=null?start:indexList.getFirst();
        while(item!=null&&count<maxRange){
            if(rangeStart==null){
                rangeStart=item;
            }
            rangeStop=item;
            Object key = getKey(item);
            put(key,item);
            item=getNextEntry(item);
        }
        return count;
    }

    /**
     * 
     * @see java.util.Map#clear()
     */
    public void clear(){
        internalMap.clear();
        indexList.clear();
        rangeStart=null;
        rangeStop=null;
    }

    
    /**
     * @return the indexList
     */
    protected IndexLinkedList getIndexList(){
        return this.indexList;
    }
    
    protected Object getKey(IndexItem item){
        StoreLocation data=item.getKeyDataItem();
        try{
            return keyDataManager.readItem(keyMarshaller,data);
        }catch(IOException e){
            log.error("Failed to get key for "+item,e);
            throw new RuntimeStoreException(e);
        }
    }
    
    protected IndexItem getNextEntry(IndexItem item) {
        return indexList.getNextEntry(item);
    }

}
