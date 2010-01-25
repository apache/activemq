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
import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * persist pending messages pending message (messages awaiting dispatch to a
 * consumer) cursor
 * 
 * @version $Revision: 474985 $
 */
class QueueStorePrefetch extends AbstractStoreCursor {
    private static final Log LOG = LogFactory.getLog(QueueStorePrefetch.class);
    private final MessageStore store;
   
    /**
     * Construct it
     * @param queue
     */
    public QueueStorePrefetch(Queue queue) {
        super(queue);
        this.store = queue.getMessageStore();

    }

    public boolean recoverMessageReference(MessageId messageReference) throws Exception {
        Message msg = this.store.getMessage(messageReference);
        if (msg != null) {
            return recoverMessage(msg);
        } else {
            String err = "Failed to retrieve message for id: " + messageReference;
            LOG.error(err);
            throw new IOException(err);
        }
    }

   
        
    @Override
    protected synchronized int getStoreSize() {
        try {
            int result = this.store.getMessageCount();
            return result;
            
        } catch (IOException e) {
            LOG.error("Failed to get message count", e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected synchronized boolean isStoreEmpty() {
        try {
            return this.store.isEmpty();
            
        } catch (Exception e) {
            LOG.error("Failed to get message count", e);
            throw new RuntimeException(e);
        }
    }
    
    @Override
    protected void resetBatch() {
        this.store.resetBatching();
    }
    
    @Override
    protected void setBatch(MessageId messageId) throws Exception {
        store.setBatch(messageId);
        batchResetNeeded = false;
    }

    
    @Override
    protected void doFillBatch() throws Exception {
        this.store.recoverNextMessages(this.maxBatchSize, this);
    }

    @Override
    public String toString() {
        return "QueueStorePrefetch" + System.identityHashCode(this);
    }

}
