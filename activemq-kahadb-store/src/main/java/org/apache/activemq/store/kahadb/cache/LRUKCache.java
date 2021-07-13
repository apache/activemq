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
package org.apache.activemq.store.kahadb.cache;

import org.apache.activemq.util.LRUCache;

import java.util.*;
import java.util.function.Predicate;
import java.util.logging.Logger;

/**
 * abstract:
 *   improved lru-k cache based on lru simple cache, a fifo linkedlist was add here, which store
 * key not hit freqTh times.
 * otherwise, if the key is queryed by freqth times, it will be moved from linkedlist to simplecache
 *
 * The difference between LRUKCache and simplelrucahce was tested in
 *
 * @see org.apache.activemq.store.kahadb.cache.LRUKCachePerformanceTest
 * ,which also show why LRUKCache can be used!
 *
 * how to config:
 *  you can enable this cache to config in activemq.xml like below:
 *     <amq:kahaDB journalMaxFileLength="10mb" enableIndexPageCaching="true" useLRUKEvication="true" lrukThresholdValue="2"/>
 *     two param is support:
 *       1. set useLRUKEvication=true, enable lruk cache
 *       2. lrukThresholdValue set the threshold to make element in cache moved from
 *             linkedlist(low level) to simplecache (high level)
 *          default value is 2
 *
 * @param <Key>
 * @param <Value>
 */
public class LRUKCache<Key, Value> implements Map<Key, Value> {

    /**
     * simple lru cache
     */
    private LRUCache<Key, CacheNode> lruCache;

    /**
     * record history visit
     */
    private LinkedList<CacheNode> fifoList;

    /**
     * cache capacity
     */
    private int capacity;

    /**
     * history fifo number
     */
    private int size;

    /**
     * threshold value for moving from fifo to lrucache
     */
    private int threshold;

    public LRUKCache(){
        this(1000, 2);
    }

    public LRUKCache(int capacity, int threshold){
        this.capacity = 2 * capacity;
        this.threshold = threshold;
        init();
    }

    private void init(){
        this.size = 0;
        fifoList = new LinkedList<CacheNode>();
        lruCache = new LRUCache<>(this.capacity);
    }

    private class CacheNode{
        private Key key;
        private int freq;
        private Value value;

        CacheNode(Key key, int freq, Value value){
            this.key = key;
            this.freq = freq;
            this.value = value;
        }

    }

    @Override
    public int size() {
        return this.size + lruCache.size();
    }

    @Override
    public boolean isEmpty() {
        return this.size == 0 && lruCache.isEmpty();
    }

    @Override
    public boolean containsKey(Object key) {
        Predicate<CacheNode> predicate = (CacheNode node)-> {
            return node.key.equals(key);
        };
        return lruCache.containsKey(key) || findKeyFromFIFOList(predicate) != null;
    }

    @Override
    public boolean containsValue(Object value) {
        Predicate<CacheNode> predicate = (CacheNode node)-> {
            return node.value.equals(value);
        };

        for(Entry<Key, CacheNode> entry : lruCache.entrySet()){
            CacheNode cacheNode = entry.getValue();
            if(cacheNode.value.equals(value)){
                return true;
            }
        }

        return findKeyFromFIFOList(predicate) != null;
    }

    public CacheNode get(Object key, boolean increaseFreq) {
        //get cachenode from lrucache first
        CacheNode cacheNode = lruCache.get(key);
        if(cacheNode != null){
            return cacheNode;
        }

        Predicate<CacheNode> predicate = (CacheNode node)-> {
            return node.key.equals(key);
        };

        //then get cachenode from fifo linkedlist
        cacheNode = findKeyFromFIFOList(predicate);
        if(cacheNode != null){
            if(increaseFreq){
                if(++cacheNode.freq >= threshold){
                    moveFromFifoListToLruCache(cacheNode);
                }
            }
            return cacheNode;
        }

        return null;
    }

    @Override
    public Value get(Object key) {
        Value value = null;
        CacheNode cacheNode = get(key, true);
        if(cacheNode != null){
            value = cacheNode.value;
        }

        return value;
    }

    private void moveFromFifoListToLruCache(CacheNode cacheNode){
        fifoList.remove(cacheNode);
        this.size--;
        lruCache.put(cacheNode.key, cacheNode);
    }


    @Override
    public Value put(Key key, Value value) {

        if(this.size + lruCache.size() > this.capacity){
            evictFifoList();
        }

        Value oldValue = null;

        CacheNode cacheNode = get(key, false);

        if(cacheNode != null){
            oldValue = cacheNode.value;
            cacheNode.value = value;
        }else {
            fifoList.offerFirst(new CacheNode(key, 1, value));
            this.size++;
        }
        return oldValue;
    }

    private void evictFifoList(){
        if(!fifoList.isEmpty()){
            fifoList.removeLast();
            this.size--;
        }
    }

    private CacheNode findKeyFromFIFOList(Predicate<CacheNode> predicate){
       Iterator<CacheNode> iterator = fifoList.iterator();
       while (iterator.hasNext()){
           CacheNode cacheNode = iterator.next();
           if(predicate.test(cacheNode)){
               return cacheNode;
           }
       }

       return null;
    }

    @Override
    public Value remove(Object key) {

        CacheNode cacheNode = lruCache.remove(key);
        if(cacheNode != null){
            return cacheNode.value;
        }

        Iterator<CacheNode> iterator = fifoList.iterator();
        while (iterator.hasNext()){
            cacheNode = iterator.next();
            if(cacheNode.key.equals(key)){
                iterator.remove();
                return cacheNode.value;
            }
        }

        return null;
    }

    @Override
    public void putAll(Map<? extends Key, ? extends Value> m) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        this.size = 0;
        lruCache.clear();
        fifoList.clear();
    }

    @Override
    public Set<Key> keySet() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Collection<Value> values() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Set<Entry<Key, Value>> entrySet() {
        throw new UnsupportedOperationException();
    }
}
