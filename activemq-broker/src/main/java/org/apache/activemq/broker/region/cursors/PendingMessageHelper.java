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
package org.apache.activemq.broker.region.cursors;

import java.util.Map;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.management.SizeStatisticImpl;

/**
 *
 *
 */
public class PendingMessageHelper {

    private final Map<MessageId, PendingNode> map;
    private final SizeStatisticImpl messageSize;

    public PendingMessageHelper(Map<MessageId, PendingNode> map,
            SizeStatisticImpl messageSize) {
        super();
        this.map = map;
        this.messageSize = messageSize;
    }

    public void addToMap(MessageReference message, PendingNode node) {
        PendingNode previous = this.map.put(message.getMessageId(), node);
        if (previous != null) {
            try {
                messageSize.addSize(-previous.getMessage().getSize());
            } catch (Exception e) {
              //expected for NullMessageReference
            }
        }
        try {
            messageSize.addSize(message.getSize());
        } catch (Exception e) {
          //expected for NullMessageReference
        }
    }

    public PendingNode removeFromMap(MessageReference message) {
        PendingNode removed = this.map.remove(message.getMessageId());
        if (removed != null) {
            try {
                messageSize.addSize(-removed.getMessage().getSize());
            } catch (Exception e) {
                //expected for NullMessageReference
            }
        }
        return removed;
    }
}
