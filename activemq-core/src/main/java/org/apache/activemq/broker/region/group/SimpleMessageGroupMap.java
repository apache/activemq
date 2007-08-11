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

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.activemq.command.ConsumerId;

/**
 * A simple implementation which tracks every individual GroupID value but
 * which can become a memory leak if clients die before they complete a message
 * group.
 * 
 * @version $Revision$
 */
public class SimpleMessageGroupMap implements MessageGroupMap {
    private Map<String, ConsumerId> map = new ConcurrentHashMap<String, ConsumerId>();
    
    public void put(String groupId, ConsumerId consumerId) {
        map.put(groupId, consumerId);
    }

    public ConsumerId get(String groupId) {
        return map.get(groupId);
    }

    public ConsumerId removeGroup(String groupId) {
        return map.remove(groupId);
    }

    public MessageGroupSet removeConsumer(ConsumerId consumerId) {
        SimpleMessageGroupSet ownedGroups = new SimpleMessageGroupSet();
        for (Iterator<String> iter = map.keySet().iterator(); iter.hasNext();) {
            String group = iter.next();
            ConsumerId owner = map.get(group);
            if (owner.equals(consumerId)) {
                ownedGroups.add(group);
                iter.remove();
            }
        }
        return ownedGroups;
    }

    public String toString() {
        return "message groups: " + map.size();
    }

}
