/**
 * 
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package org.apache.activemq.broker.region.cursors;

import org.apache.activemq.broker.region.MessageReference;

/**
 * Interface to pending message (messages awaiting disptach to a consumer) cursor
 * 
 * @version $Revision$
 */
public interface PendingMessageCursor{
    /**
     * @return true if there are no pending messages
     */
    public boolean isEmpty();
    
    /**
     * reset the cursor
     *
     */
    public void reset();

    /**
     * add message to await dispatch
     * @param node
     */
    public void addMessageLast(MessageReference node);
    
    /**
     * add message to await dispatch
     * @param node
     */
    public void addMessageFirst(MessageReference node);

    /**
     * @return true if there pending messages to dispatch
     */
    public boolean hasNext();

    /**
     * @return the next pending message
     */
    public MessageReference next();

    /**
     * remove the message at the cursor position
     * 
     */
    public void remove();

    /**
     * @return the number of pending messages
     */
    public int size();

    /**
     * clear all pending messages
     * 
     */
    public void clear();
}
