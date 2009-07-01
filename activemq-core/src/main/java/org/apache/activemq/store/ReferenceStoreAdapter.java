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
package org.apache.activemq.store;

import java.io.IOException;
import java.util.Map;
import java.util.Set;

import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.amq.AMQTx;

/**
 * Adapter to the actual persistence mechanism used with ActiveMQ
 * 
 * @version $Revision: 1.3 $
 */
public interface ReferenceStoreAdapter extends PersistenceAdapter {

    /**
     * Factory method to create a new queue message store with the given
     * destination name
     * 
     * @param destination
     * @return the QueueReferenceStore
     * @throws IOException
     */
    ReferenceStore createQueueReferenceStore(ActiveMQQueue destination) throws IOException;

    /**
     * Factory method to create a new topic message store with the given
     * destination name
     * 
     * @param destination
     * @return the TopicRefererenceStore
     * @throws IOException
     */
    TopicReferenceStore createTopicReferenceStore(ActiveMQTopic destination) throws IOException;

    /**
     * @return Set of File ids in use
     * @throws IOException
     */
    Set<Integer> getReferenceFileIdsInUse() throws IOException;

    /**
     * If the store isn't valid, it can be recoverd at start-up
     * 
     * @return true if the reference store is in a consistent state
     */
    boolean isStoreValid();

    /**
     * called by recover to clear out message references
     * 
     * @throws IOException
     */
    void clearMessages() throws IOException;

    /**
     * recover any state
     * 
     * @throws IOException
     */
    void recoverState() throws IOException;

    /**
     * Save prepared transactions
     * 
     * @param map
     * @throws IOException
     */
    void savePreparedState(Map<TransactionId, AMQTx> map) throws IOException;

    /**
     * @return saved prepared transactions
     * @throws IOException
     */
    Map<TransactionId, AMQTx> retrievePreparedState() throws IOException;
    
    /**
     * @return the maxDataFileLength
     */
    long getMaxDataFileLength();
    
    /**
     * set the max data length of a reference data log - if used
     * @param maxDataFileLength
     */
    void setMaxDataFileLength(long maxDataFileLength);
    
    /**
     * Recover particular subscription. Used for recovery of durable consumers
     * @param info
     * @throws IOException
     */
    void recoverSubscription(SubscriptionInfo info) throws IOException;
}
