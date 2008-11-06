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

import java.io.IOException;
import java.util.LinkedHashMap;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.filter.NonCachedMessageEvaluationContext;
import org.apache.activemq.store.TopicMessageStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * perist pendingCount messages pendingCount message (messages awaiting disptach
 * to a consumer) cursor
 * 
 * @version $Revision$
 */
class TopicStorePrefetch extends AbstractStoreCursor {
    private static final Log LOG = LogFactory.getLog(TopicStorePrefetch.class);
    private TopicMessageStore store;
    private final LinkedHashMap<MessageId,Message> batchList = new LinkedHashMap<MessageId,Message> ();
    private String clientId;
    private String subscriberName;
    private Subscription subscription;
    
    /**
     * @param topic
     * @param clientId
     * @param subscriberName
     */
    public TopicStorePrefetch(Subscription subscription,Topic topic, String clientId, String subscriberName) {
        super(topic);
        this.subscription=subscription;
        this.store = (TopicMessageStore)topic.getMessageStore();
        this.clientId = clientId;
        this.subscriberName = subscriberName;
        this.maxProducersToAudit=32;
        this.maxAuditDepth=10000;
    }

    public boolean recoverMessageReference(MessageId messageReference) throws Exception {
        // shouldn't get called
        throw new RuntimeException("Not supported");
    }
    
        
    public synchronized boolean recoverMessage(Message message, boolean cached) throws Exception {
        MessageEvaluationContext messageEvaluationContext = new NonCachedMessageEvaluationContext();
        messageEvaluationContext.setMessageReference(message);
        if (this.subscription.matches(message, messageEvaluationContext)) {
            return super.recoverMessage(message, cached);
        }
        return false;
        
    }

   
    protected synchronized int getStoreSize() {
        try {
            return store.getMessageCount(clientId, subscriberName);
        } catch (IOException e) {
            LOG.error(this + " Failed to get the outstanding message count from the store", e);
            throw new RuntimeException(e);
        }
    }

            
    protected void resetBatch() {
        this.store.resetBatching(clientId, subscriberName);
    }
    
    protected void doFillBatch() throws Exception {
        this.store.recoverNextMessages(clientId, subscriberName,
                maxBatchSize, this);
    }

    public String toString() {
        return "TopicStorePrefetch" + System.identityHashCode(this) + "(" + clientId + "," + subscriberName + ")";
    }
}
