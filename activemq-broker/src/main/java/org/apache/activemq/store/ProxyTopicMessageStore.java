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

/**
 * A simple proxy that delegates to another MessageStore.
 */
public class ProxyTopicMessageStore extends ProxyMessageStore implements TopicMessageStore  {

    public ProxyTopicMessageStore(TopicMessageStore delegate) {
        super(delegate);
    }

    public MessageStore getDelegate() {
        return delegate;
    }

    @Override
    public void addMessage(ConnectionContext context, Message message) throws IOException {
        delegate.addMessage(context, message);
    }

    @Override
    public void addMessage(ConnectionContext context, Message message, boolean canOptimizeHint) throws IOException {
       delegate.addMessage(context, message, canOptimizeHint);
    }

    @Override
    public Message getMessage(MessageId identity) throws IOException {
        return delegate.getMessage(identity);
    }

    @Override
    public void recover(MessageRecoveryListener listener) throws Exception {
        delegate.recover(listener);
    }

    @Override
    public void removeAllMessages(ConnectionContext context) throws IOException {
        delegate.removeAllMessages(context);
    }

    @Override
    public void removeMessage(ConnectionContext context, MessageAck ack) throws IOException {
        delegate.removeMessage(context, ack);
    }

    @Override
    public void start() throws Exception {
        delegate.start();
    }

    @Override
    public void stop() throws Exception {
        delegate.stop();
    }

    @Override
    public SubscriptionInfo lookupSubscription(String clientId, String subscriptionName) throws IOException {
        return ((TopicMessageStore)delegate).lookupSubscription(clientId, subscriptionName);
    }

    @Override
    public void acknowledge(ConnectionContext context, String clientId, String subscriptionName,
                            MessageId messageId, MessageAck ack) throws IOException {
        ((TopicMessageStore)delegate).acknowledge(context, clientId, subscriptionName, messageId, ack);
    }

    @Override
    public void addSubscription(SubscriptionInfo subscriptionInfo, boolean retroactive) throws IOException {
        ((TopicMessageStore)delegate).addSubscription(subscriptionInfo, retroactive);
    }

    @Override
    public void deleteSubscription(String clientId, String subscriptionName) throws IOException {
        ((TopicMessageStore)delegate).deleteSubscription(clientId, subscriptionName);
    }

    @Override
    public void recoverSubscription(String clientId, String subscriptionName, MessageRecoveryListener listener)
        throws Exception {
        ((TopicMessageStore)delegate).recoverSubscription(clientId, subscriptionName, listener);
    }

    @Override
    public void recoverNextMessages(String clientId, String subscriptionName, int maxReturned,
                                    MessageRecoveryListener listener) throws Exception {
        ((TopicMessageStore)delegate).recoverNextMessages(clientId, subscriptionName, maxReturned, listener);
    }

    @Override
    public void resetBatching(String clientId, String subscriptionName) {
        ((TopicMessageStore)delegate).resetBatching(clientId, subscriptionName);
    }

    @Override
    public ActiveMQDestination getDestination() {
        return delegate.getDestination();
    }

    @Override
    public SubscriptionInfo[] getAllSubscriptions() throws IOException {
        return ((TopicMessageStore)delegate).getAllSubscriptions();
    }

    @Override
    public void setMemoryUsage(MemoryUsage memoryUsage) {
        delegate.setMemoryUsage(memoryUsage);
    }

    @Override
    public int getMessageCount(String clientId, String subscriberName) throws IOException {
        return ((TopicMessageStore)delegate).getMessageCount(clientId, subscriberName);
    }

    @Override
    public int getMessageCount() throws IOException {
        return delegate.getMessageCount();
    }

    @Override
    public long getMessageSize() throws IOException {
        return delegate.getMessageSize();
    }

    @Override
    public void recoverNextMessages(int maxReturned, MessageRecoveryListener listener) throws Exception {
        delegate.recoverNextMessages(maxReturned, listener);
    }

    @Override
    public void dispose(ConnectionContext context) {
        delegate.dispose(context);
    }

    @Override
    public void resetBatching() {
        delegate.resetBatching();
    }

    @Override
    public void setBatch(MessageId messageId) throws Exception {
        delegate.setBatch(messageId);
    }

    @Override
    public boolean isEmpty() throws Exception {
        return delegate.isEmpty();
     }

    @Override
    public ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, Message message) throws IOException {
        return delegate.asyncAddTopicMessage(context, message);
     }

    @Override
    public ListenableFuture<Object> asyncAddTopicMessage(ConnectionContext context, Message message, boolean canOptimizeHint) throws IOException {
        return delegate.asyncAddTopicMessage(context,message, canOptimizeHint);
    }

    @Override
    public ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, Message message) throws IOException {
        return delegate.asyncAddQueueMessage(context, message);
    }

    @Override
    public ListenableFuture<Object> asyncAddQueueMessage(ConnectionContext context, Message message, boolean canOptimizeHint) throws IOException {
        return delegate.asyncAddQueueMessage(context,message, canOptimizeHint);
    }

    @Override
    public void removeAsyncMessage(ConnectionContext context, MessageAck ack) throws IOException {
        delegate.removeAsyncMessage(context, ack);
    }

    @Override
    public void setPrioritizedMessages(boolean prioritizedMessages) {
        delegate.setPrioritizedMessages(prioritizedMessages);
    }

    @Override
    public boolean isPrioritizedMessages() {
        return delegate.isPrioritizedMessages();
    }

    @Override
    public void updateMessage(Message message) throws IOException {
        delegate.updateMessage(message);
    }

    @Override
    public void registerIndexListener(IndexListener indexListener) {
        delegate.registerIndexListener(indexListener);
    }

    @Override
    public MessageStoreStatistics getMessageStoreStatistics() {
        return delegate.getMessageStoreStatistics();
    }

    /* (non-Javadoc)
     * @see org.apache.activemq.store.TopicMessageStore#getMessageSize(java.lang.String, java.lang.String)
     */
    @Override
    public long getMessageSize(String clientId, String subscriberName)
            throws IOException {
        return ((TopicMessageStore)delegate).getMessageSize(clientId, subscriberName);
    }

    @Override
    public MessageStoreSubscriptionStatistics getMessageStoreSubStatistics() {
        return ((TopicMessageStore)delegate).getMessageStoreSubStatistics();
    }
}
