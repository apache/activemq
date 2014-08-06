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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.activemq.broker.Broker;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.broker.region.DurableTopicSubscription;
import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.broker.region.SubscriptionRecovery;
import org.apache.activemq.broker.region.Topic;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.filter.DestinationFilter;

/**
 * This implementation of {@link org.apache.activemq.broker.region.policy.SubscriptionRecoveryPolicy} will only keep the
 * last non-zero length message with the {@link org.apache.activemq.command.ActiveMQMessage}.RETAIN_PROPERTY.
 *
 * @org.apache.xbean.XBean
 *
 */
public class RetainedMessageSubscriptionRecoveryPolicy implements SubscriptionRecoveryPolicy {

    public static final String RETAIN_PROPERTY = "ActiveMQ.Retain";
    public static final String RETAINED_PROPERTY = "ActiveMQ.Retained";
    private volatile MessageReference retainedMessage;
    private SubscriptionRecoveryPolicy wrapped;

    public RetainedMessageSubscriptionRecoveryPolicy(SubscriptionRecoveryPolicy wrapped) {
        this.wrapped = wrapped;
    }

    public boolean add(ConnectionContext context, MessageReference node) throws Exception {
        final Message message = node.getMessage();
        final Object retainValue = message.getProperty(RETAIN_PROPERTY);
        // retain property set to true
        final boolean retain = retainValue != null && Boolean.parseBoolean(retainValue.toString());
        if (retain) {
            if (message.getContent().getLength() > 0) {
                // non zero length message content
                retainedMessage = message.copy();
                retainedMessage.getMessage().removeProperty(RETAIN_PROPERTY);
                retainedMessage.getMessage().setProperty(RETAINED_PROPERTY, true);
            } else {
                // clear retained message
                retainedMessage = null;
            }
            // TODO should we remove the publisher's retain property??
            node.getMessage().removeProperty(RETAIN_PROPERTY);
        }
        return wrapped == null ? true : wrapped.add(context, node);
    }

    public void recover(ConnectionContext context, Topic topic, SubscriptionRecovery sub) throws Exception {
        // Re-dispatch the last retained message seen.
        if (retainedMessage != null) {
            sub.addRecoveredMessage(context, retainedMessage);
        }
        if (wrapped != null) {
            // retain default ActiveMQ behaviour of recovering messages only for empty durable subscriptions
            boolean recover = true;
            if (sub instanceof DurableTopicSubscription && !((DurableTopicSubscription)sub).isEmpty(topic)) {
                recover = false;
            }
            if (recover) {
                wrapped.recover(context, topic, sub);
            }
        }
    }

    public void start() throws Exception {
        if (wrapped != null) {
            wrapped.start();
        }
    }

    public void stop() throws Exception {
        if (wrapped != null) {
            wrapped.stop();
        }
    }

    public Message[] browse(ActiveMQDestination destination) throws Exception {
        final List<Message> result = new ArrayList<Message>();
        if (retainedMessage != null) {
            DestinationFilter filter = DestinationFilter.parseFilter(destination);
            if (filter.matches(retainedMessage.getMessage().getDestination())) {
                result.add(retainedMessage.getMessage());
            }
        }
        Message[] messages = result.toArray(new Message[result.size()]);
        if (wrapped != null) {
            final Message[] wrappedMessages = wrapped.browse(destination);
            if (wrappedMessages != null && wrappedMessages.length > 0) {
                final int origLen = messages.length;
                messages = Arrays.copyOf(messages, origLen + wrappedMessages.length);
                System.arraycopy(wrappedMessages, 0, messages, origLen, wrappedMessages.length);
            }
        }
        return messages;
    }

    public SubscriptionRecoveryPolicy copy() {
        return new RetainedMessageSubscriptionRecoveryPolicy(wrapped);
    }
    
    public void setBroker(Broker broker) {        
    }

    public void setWrapped(SubscriptionRecoveryPolicy wrapped) {
        this.wrapped = wrapped;
    }

    public SubscriptionRecoveryPolicy getWrapped() {
        return wrapped;
    }
}
