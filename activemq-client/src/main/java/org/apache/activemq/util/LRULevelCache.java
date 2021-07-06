package org.apache.activemq.util;

import java.util.*;

/**
 * 解决LRU缓存的预热过低的问题
 * 使用了多级缓存，设置阈值的方法，提供了缓存晋升的机制
 * 查询的时候先从高级别的缓存去查，如果不存在，则降低level，如果存在，则增加访问次数
 * @author mujianjiang
 * @version 1.0
 * @date 2021/3/11 0011 11:15
 */
public class LRULevelCache<Key,Value> extends LinkedHashMap<Key, Value>{

    private int maxCacheSize;

    private LRULevelCache<Key, Value> upCache;

    private Map<Key, Integer> map = new HashMap<>();

    private int threadHold;

    private final static int DEFAULT_THREAD_HOLD = 5;

    private LRULevelCache<Key, Value> downCache;

    private int level;

    private LRULevelCache(int maxCacheSize){
        this(maxCacheSize, null);
    }

    public LRULevelCache(int maxCacheSize, int level){
        this(maxCacheSize, DEFAULT_THREAD_HOLD, level);
    }

    public LRULevelCache(int maxCacheSize, int threadHold, int level){
        this(maxCacheSize);
        this.threadHold = threadHold;
        this.level = level;
        init();
    }

    private void init(){
        for (int i = 1; i <= level; i++) {
            LRULevelCache<Key, Value> cacheLoop = new LRULevelCache<Key, Value>(maxCacheSize);
            cacheLoop.setDownCache(this);
            setUpCache(cacheLoop);
        }
    }

    private LRULevelCache(int maxCacheSize, LRULevelCache<Key, Value> upCache) {
        this.maxCacheSize = maxCacheSize;
        this.upCache = upCache;
        if(upCache != null) {
            upCache.setDownCache(this);
        }
    }

    @Override
    public Value put(Key key, Value value) {
        if(map.get(key) == null) {
            super.put(key, value);
            map.put(key, 1);
        } else {
            return super.put(key, value);
        }

        return value;
    }

    @Override
    public Value get(Object key) {
        //先查父亲缓存，然后再查当前的缓存
        if(upCache != null) {
            Value value = upCache.get(key);
            if(value != null) {
                return value;
            }
        }

        Value value = super.get(key);
        if(value != null) {
            increaseFeq((Key)key, value);
            return value;
        }

        return null;
    }

    private void increaseFeq(Key key, Value value) {
        Integer nums = map.get(key);
        nums++;
        if(nums > threadHold && upCache != null) {
           moveToUp(key, value,this, upCache);
        }
        map.put(key, nums);
    }

    private void moveToUp(Key key, Value value, LRULevelCache<Key, Value> downCache, LRULevelCache<Key, Value> upCache){
        upCache.put(key, value);
        super.remove(key);
        map.remove(key);
    }

    public void setDownCache(LRULevelCache<Key, Value> downCache) {
        this.downCache = downCache;
    }

    public void setUpCache(LRULevelCache<Key, Value> upCache) {
        this.upCache = upCache;
    }

    @Override
    protected boolean removeEldestEntry(Map.Entry<Key, Value> eldest) {
        if( size() > maxCacheSize ) {
            onCacheEviction(eldest);
            return true;
        }
        return false;
    }

    protected void onCacheEviction(Map.Entry<Key, Value> eldest) {
    }

    private boolean isNull() {
        return map.isEmpty() && super.isEmpty();
    }

    @Override
    public boolean isEmpty() {
        LRULevelCache<Key, Value> temp = this;
        while (temp != null) {
            if(!temp.isNull()){
                return false;
            }
            temp= temp.upCache;
        }
        return true;
    }


    @Override
    public boolean containsKey(Object key) {
        LRULevelCache<Key, Value> temp = this;
        while (temp != null) {
            if(temp.map.containsKey(key)){
                return true;
            }
            temp= temp.upCache;
        }
        return false;
    }

    private boolean containsV(Object v) {
        return super.containsValue(v);
    }

    @Override
    public boolean containsValue(Object value) {
        LRULevelCache<Key, Value> temp = this;
        while (temp != null) {
            if(temp.containsV(value)){
                return true;
            }
            temp= temp.upCache;
        }
        return false;
    }

    private Value del(Object key){
        Integer num = null;
        if((num = map.remove(key)) != null && num > 0){
            return super.remove(key);
        }
        return null;
    }

    @Override
    public Value remove(Object key) {
        LRULevelCache<Key, Value> temp = this;
        Value value = null;
        while (temp != null) {
            if((value = temp.del(key)) != null) {
                return value;
            }
            temp= temp.upCache;
        }
        return null;
    }

    @Override
    public boolean remove(Object key, Object value) {
        return remove(key) == value;
    }

    @Override
    public void putAll(Map<? extends Key, ? extends Value> m) {
        super.putAll(m);
    }

    private void clean(){
        map.clear();
        super.clear();
    }


    @Override
    public void clear() {
        LRULevelCache<Key, Value> temp = this;
        while (temp != null) {
            temp.clean();
            temp= temp.upCache;
        }
    }

    private Set<Key> keySetOfThis(){
        return super.keySet();
    }

    @Override
    public Set<Key> keySet() {
        LRULevelCache<Key, Value> temp = this.upCache;

        Set<Key> sets = super.keySet();
        while (temp != null) {
            sets.addAll(temp.keySetOfThis());
            temp= temp.upCache;
        }

        return sets;
    }

    private Collection<Value> valuesOfThis() {
        return super.values();
    }

    @Override
    public Collection<Value> values() {
        LRULevelCache<Key, Value> temp = this.upCache;

        Collection<Value> values = super.values();
        while (temp != null) {
            values.addAll(temp.valuesOfThis());
            temp= temp.upCache;
        }

        return values;
    }

    public Set<Map.Entry<Key, Value>> entrySetOfThis() {
        return super.entrySet();
    }

    @Override
    public Set<Map.Entry<Key, Value>> entrySet() {
        LRULevelCache<Key, Value> temp = this.upCache;

        Set<Map.Entry<Key,Value>> sets = super.entrySet();
        while (temp != null) {
            sets.addAll(temp.entrySetOfThis());
            temp= temp.upCache;
        }

        return sets;
    }
}
