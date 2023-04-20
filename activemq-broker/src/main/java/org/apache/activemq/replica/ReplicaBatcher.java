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
package org.apache.activemq.replica;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQMessage;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ReplicaBatcher {

    private ReplicaPolicy replicaPolicy;

    public ReplicaBatcher(ReplicaPolicy replicaPolicy) {
        this.replicaPolicy = replicaPolicy;
    }

    @SuppressWarnings("unchecked")
    List<List<MessageReference>> batches(List<MessageReference> list) throws Exception {
        List<List<MessageReference>> result = new ArrayList<>();

        Map<String, Set<String>> destination2eventType = new HashMap<>();
        List<MessageReference> batch = new ArrayList<>();
        int batchSize = 0;
        for (MessageReference reference : list) {
            ActiveMQMessage message = (ActiveMQMessage) reference.getMessage();
            String originalDestination = message.getStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY);
            ReplicaEventType currentEventType =
                    ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));

            boolean eventTypeSwitch = false;
            if (originalDestination != null) {
                Set<String> sends = destination2eventType.computeIfAbsent(originalDestination, k -> new HashSet<>());
                if (currentEventType == ReplicaEventType.MESSAGE_SEND) {
                    sends.add(message.getStringProperty(ReplicaSupport.MESSAGE_ID_PROPERTY));
                }
                if (currentEventType == ReplicaEventType.MESSAGE_ACK) {
                    List<String> stringProperty = (List<String>) message.getProperty(ReplicaSupport.MESSAGE_IDS_PROPERTY);
                    if (sends.stream().anyMatch(stringProperty::contains)) {
                        destination2eventType.put(originalDestination, new HashSet<>());
                        eventTypeSwitch = true;
                    }
                }
            }

            boolean exceedsLength = batch.size() + 1 > replicaPolicy.getMaxBatchLength();
            boolean exceedsSize = batchSize + reference.getSize() > replicaPolicy.getMaxBatchSize();
            if (batch.size() > 0 && (exceedsLength || exceedsSize || eventTypeSwitch)) {
                result.add(batch);
                batch = new ArrayList<>();
                batchSize = 0;
            }

            batch.add(reference);
            batchSize += reference.getSize();
        }
        if (batch.size() > 0) {
            result.add(batch);
        }

        return result;
    }
}
