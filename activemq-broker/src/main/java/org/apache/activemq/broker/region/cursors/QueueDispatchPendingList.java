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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;

/**
 * An abstraction that keeps the correct order of messages that need to be dispatched
 * to consumers, but also hides the fact that there might be redelivered messages that
 * should be dispatched ahead of any other paged in messages.
 *
 * Direct usage of this class is recommended as you can control when redeliveries need
 * to be added vs regular pending messages (the next set of messages that can be dispatched)
 *
 * Created by ceposta
 * <a href="http://christianposta.com/blog>http://christianposta.com/blog</a>.
 */
public class QueueDispatchPendingList implements PendingList {

    private PendingList pagedInPendingDispatch = new OrderedPendingList();
    private PendingList redeliveredWaitingDispatch = new OrderedPendingList();
    private boolean prioritized = false;


    @Override
    public boolean isEmpty() {
        return pagedInPendingDispatch.isEmpty() && redeliveredWaitingDispatch.isEmpty();
    }

    @Override
    public void clear() {
        pagedInPendingDispatch.clear();
        redeliveredWaitingDispatch.clear();
    }

    /**
     * Messages added are added directly to the pagedInPendingDispatch set of messages. If
     * you're trying to add a message that is marked redelivered add it using addMessageForRedelivery()
     * method
     * @param message
     *      The MessageReference that is to be added to this list.
     *
     * @return the pending node.
     */
    @Override
    public PendingNode addMessageFirst(MessageReference message) {
        return pagedInPendingDispatch.addMessageFirst(message);
    }

    /**
     * Messages added are added directly to the pagedInPendingDispatch set of messages. If
     * you're trying to add a message that is marked redelivered add it using addMessageForRedelivery()
     * method
     * @param message
     *      The MessageReference that is to be added to this list.
     *
     * @return the pending node.
     */
    @Override
    public PendingNode addMessageLast(MessageReference message) {
        return pagedInPendingDispatch.addMessageLast(message);
    }

    @Override
    public PendingNode remove(MessageReference message) {
        if (pagedInPendingDispatch.contains(message)) {
            return pagedInPendingDispatch.remove(message);
        } else if (redeliveredWaitingDispatch.contains(message)) {
            return redeliveredWaitingDispatch.remove(message);
        }
        return null;
    }

    @Override
    public int size() {
        return pagedInPendingDispatch.size() + redeliveredWaitingDispatch.size();
    }

    @Override
    public long messageSize() {
        return pagedInPendingDispatch.messageSize() + redeliveredWaitingDispatch.messageSize();
    }

    @Override
    public Iterator<MessageReference> iterator() {
        if (prioritized && hasRedeliveries()) {
            final QueueDispatchPendingList delegate = this;
            final PrioritizedPendingList  priorityOrderedRedeliveredAndPending = new PrioritizedPendingList();
            priorityOrderedRedeliveredAndPending.addAll(redeliveredWaitingDispatch);
            priorityOrderedRedeliveredAndPending.addAll(pagedInPendingDispatch);

            return new Iterator<MessageReference>() {

                Iterator<MessageReference> combinedIterator = priorityOrderedRedeliveredAndPending.iterator();
                MessageReference current = null;

                @Override
                public boolean hasNext() {
                    return combinedIterator.hasNext();
                }

                @Override
                public MessageReference next() {
                    current = combinedIterator.next();
                    return current;
                }

                @Override
                public void remove() {
                    if (current!=null) {
                        delegate.remove(current);
                    }
                }
            };

        } else {

            return new Iterator<MessageReference>() {

                Iterator<MessageReference> redeliveries = redeliveredWaitingDispatch.iterator();
                Iterator<MessageReference> pendingDispatch = pagedInPendingDispatch.iterator();
                Iterator<MessageReference> current = redeliveries;


                @Override
                public boolean hasNext() {
                    if (!redeliveries.hasNext() && (current == redeliveries)) {
                        current = pendingDispatch;
                    }
                    return current.hasNext();
                }

                @Override
                public MessageReference next() {
                    return current.next();
                }

                @Override
                public void remove() {
                    current.remove();
                }
            };
        }
    }

    @Override
    public boolean contains(MessageReference message) {
        return pagedInPendingDispatch.contains(message) || redeliveredWaitingDispatch.contains(message);
    }

    @Override
    public Collection<MessageReference> values() {
        List<MessageReference> messageReferences = new ArrayList<MessageReference>();
        Iterator<MessageReference> iterator = iterator();
        while (iterator.hasNext()) {
            messageReferences.add(iterator.next());
        }
        return messageReferences;
    }

    @Override
    public void addAll(PendingList pendingList) {
        pagedInPendingDispatch.addAll(pendingList);
    }

    @Override
    public MessageReference get(MessageId messageId) {
        MessageReference rc = pagedInPendingDispatch.get(messageId);
        if (rc == null) {
            return redeliveredWaitingDispatch.get(messageId);
        }
        return rc;
    }

    public void setPrioritizedMessages(boolean prioritizedMessages) {
        prioritized = prioritizedMessages;
        if (prioritizedMessages && this.pagedInPendingDispatch instanceof OrderedPendingList) {
            pagedInPendingDispatch = new PrioritizedPendingList();
            redeliveredWaitingDispatch = new PrioritizedPendingList();
        } else if(pagedInPendingDispatch instanceof PrioritizedPendingList) {
            pagedInPendingDispatch = new OrderedPendingList();
            redeliveredWaitingDispatch = new OrderedPendingList();
        }
    }

    public boolean hasRedeliveries(){
        return !redeliveredWaitingDispatch.isEmpty();
    }

    public void addForRedelivery(List<MessageReference> list, boolean noConsumers) {
        if (noConsumers && redeliveredWaitingDispatch instanceof OrderedPendingList && willBeInOrder(list)) {
            // a single consumer can expect repeatable redelivery order irrespective
            // of transaction or prefetch boundaries
            ((OrderedPendingList)redeliveredWaitingDispatch).insertAtHead(list);
        } else {
            for (MessageReference ref : list) {
                redeliveredWaitingDispatch.addMessageLast(ref);
            }
        }
    }

    private boolean willBeInOrder(List<MessageReference> list) {
        // for a single consumer inserting at head will be in order w.r.t brokerSequence but
        // will not be if there were multiple consumers in the mix even if this is the last
        // consumer to close (noConsumers==true)
        return !redeliveredWaitingDispatch.isEmpty() && list != null && !list.isEmpty() &&
            redeliveredWaitingDispatch.iterator().next().getMessageId().getBrokerSequenceId() > list.get(list.size() - 1).getMessageId().getBrokerSequenceId();
    }
}
