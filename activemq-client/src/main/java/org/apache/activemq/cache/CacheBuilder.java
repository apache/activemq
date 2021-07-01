package org.apache.activemq.cache;

import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.LRUKCache;

import java.util.Map;

/**
 * build mode
 * build cache according to cache type
 * @param <K>
 * @param <V>
 */
public class CacheBuilder<K, V> {

    private int cacheSize;

    private int threadSize;

    private CacheType cacheType;

    public CacheBuilder setCacheType(CacheType cacheType){
        this.cacheType = cacheType;
        return this;
    }

    public CacheType getCacheType() {
        return cacheType;
    }

    public CacheBuilder setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
        return this;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public CacheBuilder setThreadSize(int threadSize) {
        this.threadSize = threadSize;
        return this;
    }

    public int getThreadSize() {
        return threadSize;
    }

    public Map<K, V> build(){
        Map<K, V> cache = null;
        switch (cacheType){
            case LURK:
                cache = new LRUKCache<>(this.cacheSize, this.threadSize);
                break;
            default:
                cache = new LRUCache<>(this.cacheSize);
        }

        return cache;
    }

}
