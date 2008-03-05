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
package org.apache.activemq.filter;

import java.io.IOException;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;

/**
 * MessageEvaluationContext is used to cache selection results. A message
 * usually has multiple selectors applied against it. Some selector have a high
 * cost of evaluating against the message. Those selectors may whish to cache
 * evaluation results associated with the message in the
 * MessageEvaluationContext.
 * 
 * @version $Revision: 1.4 $
 */
public class MessageEvaluationContext {

	protected MessageReference messageReference;
    protected boolean loaded;
    protected boolean dropped;
    protected Message message;
    protected ActiveMQDestination destination;

    public MessageEvaluationContext() {
    }

    public boolean isDropped() throws IOException {
        getMessage();
        return dropped;
    }

    public Message getMessage() throws IOException {
        if (!dropped && !loaded) {
            loaded = true;
            messageReference.incrementReferenceCount();
            message = messageReference.getMessage();
            if (message == null) {
                messageReference.decrementReferenceCount();
                dropped = true;
                loaded = false;
            }
        }
        return message;
    }

    public void setMessageReference(MessageReference messageReference) {
        if (this.messageReference != messageReference) {
            clearMessageCache();
        }
        this.messageReference = messageReference;
    }

    public void clear() {
        clearMessageCache();
        destination = null;
    }

    public ActiveMQDestination getDestination() {
        return destination;
    }

    public void setDestination(ActiveMQDestination destination) {
        this.destination = destination;
    }

    /**
     * A strategy hook to allow per-message caches to be cleared
     */
    protected void clearMessageCache() {
        if (loaded) {
            messageReference.decrementReferenceCount();
        }
        message = null;
        dropped = false;
        loaded = false;
    }
}
