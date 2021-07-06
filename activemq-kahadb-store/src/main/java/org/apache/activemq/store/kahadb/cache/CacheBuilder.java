package org.apache.activemq.store.kahadb.cache;

import org.apache.activemq.util.LRUCache;
import org.apache.activemq.util.LRUKCache;
import org.apache.activemq.util.LRULevelCache;

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

    private float lfuEvictionFactor = 0.2f;

    private float lruLoadFactor;

    private int maxCacheSize;

    private boolean accessOrder;

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

    public CacheBuilder setMaxCacheSize(int maxCacheSize) {
        this.maxCacheSize = maxCacheSize;
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

    public CacheBuilder setLfuEvictionFactor(float lfuEvictionFactor) {
        this.lfuEvictionFactor = lfuEvictionFactor;
        return this;
    }

    public CacheBuilder setLruLoadFactor(float lruLoadFactor) {
        this.lruLoadFactor = lruLoadFactor;
        return this;
    }

    public CacheBuilder setAccessOrder(boolean accessOrder) {
        this.accessOrder = accessOrder;
        return this;
    }

    public Map<K, V> build(){
        Map<K, V> cache = null;
        switch (cacheType){
            case LURK:
                cache = new LRUKCache<>(this.cacheSize, this.threadSize);
                break;
            case LRUKLEVEL:
                cache = new LRULevelCache<K, V>(this.cacheSize, threadSize);
                break;
            case LFU:
                break;
            default:
                cache = new LRUCache<>(cacheSize, maxCacheSize, lruLoadFactor, accessOrder);
        }

        return cache;
    }

}
