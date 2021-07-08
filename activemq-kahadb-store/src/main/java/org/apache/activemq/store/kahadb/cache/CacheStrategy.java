package org.apache.activemq.store.kahadb.cache;

import org.apache.activemq.store.kahadb.disk.page.Page;
import org.apache.activemq.store.kahadb.disk.page.PageFile;

import java.util.Collections;
import java.util.Map;

public class CacheStrategy<K, V> {

    public Map<K, V> createCache(PageFile pageFile){
        CacheBuilder cacheBuilder = new CacheBuilder<Long, Page>();
        Map<K, V> pageCache = null;

        if (pageFile.isUseLFRUEviction()) {
            Map<K, V> cache =
                    cacheBuilder
                            .setCacheType(CacheType.LFU)
                            .setCacheSize(0)
                            .setMaxCacheSize(pageFile.getPageCacheSize())
                            .setLfuEvictionFactor(pageFile.getLFUEvictionFactor())
                            .build();
            pageCache = Collections.synchronizedMap(cache);
        } else if(pageFile.isUseLRUKEvication()){
            Map<K, V> cache =
                    cacheBuilder
                            .setCacheType(CacheType.LURK)
                            .setCacheSize(0)
                            .setMaxCacheSize(pageFile.getPageCacheSize())
                            .setThreadSize(pageFile.getLrukThreadSize())
                            .build();
            pageCache = Collections.synchronizedMap(cache);
        } else {
            Map<K, V> cache =
                    cacheBuilder
                            .setCacheType(CacheType.LRU)
                            .setCacheSize(0)
                            .setMaxCacheSize(pageFile.getPageCacheSize())
                            .setLruLoadFactor(0.75f)
                            .setAccessOrder(true)
                            .build();
            pageCache = Collections.synchronizedMap(cache);
        }
        return pageCache;
    }

}
