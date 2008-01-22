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
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.cursors.PendingMessageCursor;

/**
 * Abstraction to allow different policies for holding messages awaiting
 * dispatch to active clients
 * 
 * @version $Revision$
 */
public interface PendingDurableSubscriberMessageStoragePolicy {

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
    PendingMessageCursor getSubscriberPendingMessageCursor(Broker broker,String clientId, String name, int maxBatchSize, Subscription sub);
}
