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
package org.apache.activemq.broker.region.policy;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedList;

import org.apache.activemq.broker.region.MessageReference;

/**
 * An eviction strategy which evicts the oldest message with the lowest priority first.
 * 
 * @org.apache.xbean.XBean
 * 
 * messageEvictionStrategy
 */
public class OldestMessageWithLowestPriorityEvictionStrategy extends MessageEvictionStrategySupport {

    public MessageReference[] evictMessages(LinkedList messages) throws IOException {
        byte lowestPriority = Byte.MAX_VALUE;
        int pivot = 0;
        Iterator iter = messages.iterator();
        for (int i = 0; iter.hasNext(); i++) {
            MessageReference reference = (MessageReference) iter.next();
            byte priority = reference.getMessage().getPriority();
            if (priority < lowestPriority) {
                lowestPriority = priority;
                pivot = i;
            }
        }
        return new MessageReference[] {(MessageReference) messages.remove(pivot)};
    }
}
