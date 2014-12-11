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

import java.util.Map;

import org.apache.activemq.broker.region.Destination;
import org.apache.activemq.command.ConsumerId;
import org.apache.activemq.memory.LRUMap;

/**
 * Uses hash-code buckets to associate consumers with sets of message group IDs.
 * 
 * 
 */
public class MessageGroupHashBucket implements MessageGroupMap {

    private final int bucketCount;
    private final ConsumerId[] consumers;
    private final LRUMap<String,String>cache;

    public MessageGroupHashBucket(int bucketCount, int cachedSize) {
        this.bucketCount = bucketCount;
        this.consumers = new ConsumerId[bucketCount];
        this.cache=new LRUMap<String,String>(cachedSize);
    }

    public synchronized void put(String groupId, ConsumerId consumerId) {
        int bucket = getBucketNumber(groupId);
        consumers[bucket] = consumerId;
        if (consumerId != null){
          cache.put(groupId,consumerId.toString());
        }
    }

    public synchronized ConsumerId get(String groupId) {
        int bucket = getBucketNumber(groupId);
        //excersise cache
        cache.get(groupId);
        return consumers[bucket];
    }

    public synchronized ConsumerId removeGroup(String groupId) {
        int bucket = getBucketNumber(groupId);
        ConsumerId answer = consumers[bucket];
        consumers[bucket] = null;
        cache.remove(groupId);
        return answer;
    }

    public synchronized MessageGroupSet removeConsumer(ConsumerId consumerId) {
        MessageGroupSet answer = null;
        for (int i = 0; i < consumers.length; i++) {
            ConsumerId owner = consumers[i];
            if (owner != null && owner.equals(consumerId)) {
                answer = createMessageGroupSet(i, answer);
                consumers[i] = null;
            }
        }
        if (answer == null) {
            // make an empty set
            answer = EmptyMessageGroupSet.INSTANCE;
        }
        return answer;
    }

    public synchronized void removeAll(){
        for (int i =0; i < consumers.length; i++){
            consumers[i] = null;
        }
    }

    @Override
    public Map<String, String> getGroups() {
        return cache;
    }

    @Override
    public String getType() {
        return "bucket";
    }

    public void setDestination(Destination destination) {}

    public int getBucketCount(){
        return bucketCount;
    }


    public String toString() {
        int count = 0;
        for (int i = 0; i < consumers.length; i++) {
            if (consumers[i] != null) {
                count++;
            }
        }
        return "active message group buckets: " + count;
    }

    protected MessageGroupSet createMessageGroupSet(int bucketNumber, final MessageGroupSet parent) {
        final MessageGroupSet answer = createMessageGroupSet(bucketNumber);
        if (parent == null) {
            return answer;
        } else {
            // union the two sets together
            return new MessageGroupSet() {
                public boolean contains(String groupID) {
                    return parent.contains(groupID) || answer.contains(groupID);
                }
            };
        }
    }

    protected MessageGroupSet createMessageGroupSet(final int bucketNumber) {
        return new MessageGroupSet() {
            public boolean contains(String groupID) {
                int bucket = getBucketNumber(groupID);
                return bucket == bucketNumber;
            }
        };
    }

    protected int getBucketNumber(String groupId) {
        int bucket = groupId.hashCode() % bucketCount;
        // bucket could be negative
        if (bucket < 0) {
            bucket *= -1;
        }
        return bucket;
    }
}
