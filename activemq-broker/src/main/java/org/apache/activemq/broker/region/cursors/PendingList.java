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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.apache.activemq.broker.region.MessageReference;
import org.apache.activemq.command.MessageId;

public interface PendingList extends Iterable<MessageReference> {

    /**
     * Returns true if there are no Messages in the PendingList currently.
     * @return true if the PendingList is currently empty.
     */
    public boolean isEmpty();

    /**
     * Discards all Messages currently held in the PendingList.
     */
    public void clear();

    /**
     * Adds the given message to the head of the list.
     *
     * @param message
     *      The MessageReference that is to be added to this list.
     *
     * @return the PendingNode that contains the newly added message.
     */
    public PendingNode addMessageFirst(MessageReference message);

    /**
     * Adds the given message to the tail of the list.
     *
     * @param message
     *      The MessageReference that is to be added to this list.
     *
     * @return the PendingNode that contains the newly added message.
     */
    public PendingNode addMessageLast(MessageReference message);

    /**
     * Removes the given MessageReference from the PendingList if it is
     * contained within.
     *
     * @param message
     *      The MessageReference that is to be removed to this list.
     *
     * @return the PendingNode that contains the removed message or null if the
     *         message was not present in this list.
     */
    public PendingNode remove(MessageReference message);

    /**
     * Returns the number of MessageReferences that are awaiting dispatch.
     * @return current count of the pending messages.
     */
    public int size();

    public long messageSize();

    /**
     * Returns an iterator over the pending Messages.  The subclass controls how
     * the returned iterator actually traverses the list of pending messages allowing
     * for the order to vary based on factors like Message priority or some other
     * mechanism.
     *
     * @return an Iterator that returns MessageReferences contained in this list.
     */
    @Override
    public Iterator<MessageReference> iterator();

    /**
     * Query the PendingList to determine if the given message is contained within.
     *
     * @param message
     *      The Message that is the target of this query.
     *
     * @return true if the MessageReference is contained in this list.
     */
    public boolean contains(MessageReference message);

    /**
     * Returns a new Collection that contains all the MessageReferences currently
     * held in this PendingList.  The elements of the list are ordered using the
     * same rules as the subclass uses for iteration.
     *
     * @return a new Collection containing this lists MessageReferences.
     */
    public Collection<MessageReference> values();

    /**
     * Adds all the elements of the given PendingList to this PendingList.
     *
     * @param pendingList
     *      The PendingList that is to be added to this collection.
     */
    public void addAll(PendingList pendingList);

    public MessageReference get(MessageId messageId);

}
