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

import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.command.MessageId;
import org.apache.activemq.command.SubscriptionInfo;
import org.apache.activemq.usage.MemoryUsage;
import org.apache.activemq.usage.SystemUsage;

/**
 * A simple proxy that delegates to another MessageStore.
 */
public class ProxyTopicMessageStore implements TopicMessageStore {

    final TopicMessageStore delegate;

    public ProxyTopicMessageStore(TopicMessageStore delegate) {
        this.delegate = delegate;
    }

    public MessageStore getDelegate() {
        return delegate;
    }

    public void addMessage(ConnectionContext context, Message message) throws IOException {
        delegate.addMessage(context, message);
    }

    public Message getMessage(MessageId identity) throws IOException {
        return delegate.getMessage(identity);
    }

    public void recover(MessageRecoveryListener listener) throws Exception {
        delegate.recover(listener);
    }

    public void removeAllMessages(ConnectionContext context) throws IOException {
        delegate.removeAllMessages(context);
    }

    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
        delegate.removeMessage(context, ack);
    }

    public void start() throws Exception {
        delegate.start();
    }

    public void stop() throws Exception {
        delegate.stop();
    }

    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return delegate.lookupSubscription(clientId, subscriptionName);
    }

    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName,
                            MessageId messageId) throws IOException {
        delegate.acknowledge(context, clientId, subscriptionName, messageId);
    }

    public void addSubsciption(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
        delegate.addSubsciption(subscriptionInfo, retroactive);
    }

    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        delegate.deleteSubscription(clientId, subscriptionName);
    }

    public void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener)
        throws Exception {
        delegate.recoverSubscription(clientId, subscriptionName, listener);
    }

    public void recoverNextMessages(String clientId, String subscriptionName, int maxReturned,
                                    MessageRecoveryListener listener) throws Exception {
        delegate.recoverNextMessages(clientId, subscriptionName, maxReturned, listener);
    }

    public void resetBatching(String clientId, String subscriptionName) {
        delegate.resetBatching(clientId, subscriptionName);
    }

    public ActiveMQDestination getDestination() {
        return delegate.getDestination();
    }

    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        return delegate.getAllSubscriptions();
    }

    public void setMemoryUsage(MemoryUsage memoryUsage) {
        delegate.setMemoryUsage(memoryUsage);
    }

    public int getMessageCount(String clientId, String subscriberName) throws IOException {
        return delegate.getMessageCount(clientId, subscriberName);
    }

    public int getMessageCount() throws IOException {
        return delegate.getMessageCount();
    }

    public void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception {
        delegate.recoverNextMessages(maxReturned, listener);

    }

    public void dispose(ConnectionContext context) {
        delegate.dispose(context);
    }

    public void resetBatching() {
        delegate.resetBatching();

    }

    public void setBatch(MessageId messageId) {
        delegate.setBatch(messageId);
    }
}
