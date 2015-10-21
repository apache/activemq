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
package org.apache.activemq.broker.region.group;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.memory.LRUMap;

/**
 * A simple implementation which tracks every individual GroupID value in a LRUCache
 * 
 * 
 */
public class CachedMessageGroupMap implements MessageGroupMap {
    private final LRUMap<String, ConsumerId> cache;
    private final int maximumCacheSize;
    Destination destination;

    CachedMessageGroupMap(int size){
      cache = new LRUMap<String, ConsumerId>(size) {
          @Override
          public boolean removeEldestEntry(final Map.Entry eldest) {
              boolean remove = super.removeEldestEntry(eldest);
              if (remove) {
                  if (destination != null) {
                      for (Subscription s : destination.getConsumers()) {
                        if (s.getConsumerInfo().getConsumerId().equals(eldest.getValue())) {
                            s.getConsumerInfo().decrementAssignedGroupCount(destination.getActiveMQDestination());
                            break;
                          }
                      }
                  }
              }
              return remove;
          }
      };
      maximumCacheSize = size;
    }
    public synchronized void put(String groupId, ConsumerId consumerId) {
        cache.put(groupId, consumerId);
    }

    public synchronized ConsumerId get(String groupId) {
        return cache.get(groupId);
    }

    public synchronized ConsumerId removeGroup(String groupId) {
        return cache.remove(groupId);
    }

    public synchronized MessageGroupSet removeConsumer(ConsumerId consumerId) {
        SimpleMessageGroupSet ownedGroups = new SimpleMessageGroupSet();
        Map<String,ConsumerId> map = new HashMap<String, ConsumerId>();
        map.putAll(cache);
        for (Iterator<String> iter = map.keySet().iterator(); iter.hasNext();) {
            String group = iter.next();
            ConsumerId owner = map.get(group);
            if (owner.equals(consumerId)) {
                ownedGroups.add(group);
            }
        }
        for (String group:ownedGroups.getUnderlyingSet()){
            cache.remove(group);
        }
        return ownedGroups;
    }


    @Override
    public synchronized void removeAll(){
        cache.clear();
        if (destination != null) {
            for (Subscription s : destination.getConsumers()) {
                s.getConsumerInfo().clearAssignedGroupCount(destination.getActiveMQDestination());
            }
        }
    }

    @Override
    public synchronized Map<String, String> getGroups() {
        Map<String,String> result = new HashMap<String,String>();
        for (Map.Entry<String,ConsumerId>entry: cache.entrySet()){
            result.put(entry.getKey(),entry.getValue().toString());
        }
        return result;
    }

    @Override
    public String getType() {
        return "cached";
    }

    public int getMaximumCacheSize(){
        return maximumCacheSize;
    }

    public String toString() {
        return "message groups: " + cache.size();
    }

    public void setDestination(Destination destination) {
        this.destination = destination;
    }
}
