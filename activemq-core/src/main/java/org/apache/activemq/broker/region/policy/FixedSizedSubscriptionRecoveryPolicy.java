/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.broker.region.policy;

import java.util.Iterator;
import java.util.List;

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.Subscription;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.filter.MessageEvaluationContext;
import org.apache.activemq.memory.list.DestinationBasedMessageList;
import org.apache.activemq.memory.list.MessageList;
import org.apache.activemq.memory.list.SimpleMessageList;

/**
 * This implementation of {@link SubscriptionRecoveryPolicy} will keep a fixed
 * amount of memory available in RAM for message history which is evicted in
 * time order.
 * 
 * @org.xbean.XBean
 * 
 * @version $Revision$
 */
public class FixedSizedSubscriptionRecoveryPolicy implements SubscriptionRecoveryPolicy {

    private MessageList buffer;
    private int maximumSize = 100 * 64 * 1024;
    private boolean useSharedBuffer = true;

    public boolean add(ConnectionContext context, MessageReference message) throws Throwable {
        buffer.add(message);
        return true;
    }

    public void recover(ConnectionContext context, Topic topic, Subscription sub) throws Throwable {
        // Re-dispatch the messages from the buffer.
        List copy = buffer.getMessages(sub);
        if( !copy.isEmpty() ) {
            MessageEvaluationContext msgContext = context.getMessageEvaluationContext();
            try {
                for (Iterator iter = copy.iterator(); iter.hasNext();) {
                    MessageReference node = (MessageReference) iter.next();
                    msgContext.setDestination(node.getRegionDestination().getActiveMQDestination());
                    msgContext.setMessageReference(node);
                    if (sub.matches(node, msgContext) ) {
                        sub.add(node);
                    }
                }
            } finally {
                msgContext.clear();
            }
        }
    }

    public void start() throws Exception {
        buffer = createMessageList();
    }

    public void stop() throws Exception {
        buffer.clear();
    }

    // Properties
    // -------------------------------------------------------------------------
    public MessageList getBuffer() {
        return buffer;
    }

    public void setBuffer(MessageList buffer) {
        this.buffer = buffer;
    }

    public int getMaximumSize() {
        return maximumSize;
    }

    /**
     * Sets the maximum amount of RAM in bytes that this buffer can hold in RAM
     */
    public void setMaximumSize(int maximumSize) {
        this.maximumSize = maximumSize;
    }

    public boolean isUseSharedBuffer() {
        return useSharedBuffer;
    }

    public void setUseSharedBuffer(boolean useSharedBuffer) {
        this.useSharedBuffer = useSharedBuffer;
    }

    // Implementation methods
    
    // -------------------------------------------------------------------------
    protected MessageList createMessageList() {
        if (useSharedBuffer) {
            return new SimpleMessageList(maximumSize);
        }
        else {
            return new DestinationBasedMessageList(maximumSize);
        }
    }
}
