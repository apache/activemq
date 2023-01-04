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

import javax.jms.JMSException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplicaBatcher {

    static final int MAX_BATCH_LENGTH = 500;
    static final int MAX_BATCH_SIZE = 5_000_000; // 5 Mb

    static List<List<MessageReference>> batches(List<MessageReference> list) throws JMSException {
        List<List<MessageReference>> result = new ArrayList<>();

        Map<String, ReplicaEventType> destination2eventType = new HashMap<>();
        List<MessageReference> batch = new ArrayList<>();
        int batchSize = 0;
        for (MessageReference reference : list) {
            ActiveMQMessage message = (ActiveMQMessage) reference.getMessage();
            String originalDestination = message.getStringProperty(ReplicaSupport.ORIGINAL_MESSAGE_DESTINATION_PROPERTY);

            boolean eventTypeSwitch = false;
            if (originalDestination != null) {
                ReplicaEventType currentEventType =
                        ReplicaEventType.valueOf(message.getStringProperty(ReplicaEventType.EVENT_TYPE_PROPERTY));
                ReplicaEventType lastEventType = destination2eventType.put(originalDestination, currentEventType);
                if (lastEventType == ReplicaEventType.MESSAGE_SEND && currentEventType == ReplicaEventType.MESSAGE_ACK) {
                    eventTypeSwitch = true;
                }
            }

            boolean exceedsLength = batch.size() + 1 > MAX_BATCH_LENGTH;
            boolean exceedsSize = batchSize + reference.getSize() > MAX_BATCH_SIZE;
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
