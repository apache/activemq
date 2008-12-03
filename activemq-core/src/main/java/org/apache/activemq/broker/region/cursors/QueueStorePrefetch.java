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
import java.io.InterruptedIOException;

import org.apache.activemq.broker.region.Queue;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.store.MessageStore;
import org.apache.activemq.store.amq.AMQMessageStore;
import org.apache.activemq.store.kahadaptor.KahaReferenceStore;
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
    private MessageStore store;
   
    /**
     * Construct it
     * @param queue
     */
    public QueueStorePrefetch(Queue queue) {
        super(queue);
        this.store = (MessageStore)queue.getMessageStore();

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

   
        
    protected synchronized int getStoreSize() {
        try {
            return this.store.getMessageCount();
        } catch (IOException e) {
            LOG.error("Failed to get message count", e);
            throw new RuntimeException(e);
        }
    }
    
    protected void resetBatch() {
        this.store.resetBatching();
    }
    
    protected void setBatch(MessageId messageId) {
        AMQMessageStore amqStore = (AMQMessageStore) store;
        try {
            amqStore.flush();
        } catch (InterruptedIOException e) {
            LOG.debug("flush on setBatch resulted in exception", e);        
        }
        KahaReferenceStore kahaStore = 
            (KahaReferenceStore) amqStore.getReferenceStore();
        kahaStore.setBatch(messageId);
        batchResetNeeded = false;
    }

    
    protected void doFillBatch() throws Exception {
        this.store.recoverNextMessages(this.maxBatchSize, this);
    }

    public String toString() {
        return "QueueStorePrefetch" + System.identityHashCode(this);
    }

}
