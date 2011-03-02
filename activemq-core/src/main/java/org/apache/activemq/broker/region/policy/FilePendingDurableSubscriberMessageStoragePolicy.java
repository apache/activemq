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

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.cursors.AbstractPendingMessageCursor;
import org.apache.activemq.broker.region.cursors.FilePendingMessageCursor;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;

/**
 * Creates a PendIngMessageCursor for Durable subscribers *
 * 
 * @org.apache.xbean.XBean element="fileDurableSubscriberCursor"
 *                         description="Pending messages for durable subscribers
 *                         held in temporary files"
 * 
 */
public class FilePendingDurableSubscriberMessageStoragePolicy implements PendingDurableSubscriberMessageStoragePolicy {

    /**
     * Retrieve the configured pending message storage cursor;
     * @param broker 
     * 
     * @param clientId
     * @param name
     * @param maxBatchSize
     * @param sub 
     * @return the Pending Message cursor
     */
    public PendingMessageCursor getSubscriberPendingMessageCursor(Broker broker,String clientId, String name, int maxBatchSize, DurableTopicSubscription sub) {
        return new FilePendingMessageCursor(broker,name,AbstractPendingMessageCursor.isPrioritizedMessageSubscriber(broker, sub));
    }
}
