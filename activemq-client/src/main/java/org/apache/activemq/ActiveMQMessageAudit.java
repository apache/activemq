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
package org.apache.activemq;

import org.apache.activemq.command.MessageId;

/**
 * Provides basic audit functions for Messages
 * 
 * 
 */
public class ActiveMQMessageAudit extends ActiveMQMessageAuditNoSync {

    private static final long serialVersionUID = 1L;

    /**
     * Default Constructor windowSize = 2048, maximumNumberOfProducersToTrack =
     * 64
     */
    public ActiveMQMessageAudit() {
        super();
    }

    /**
     * Construct a MessageAudit
     * 
     * @param auditDepth range of ids to track
     * @param maximumNumberOfProducersToTrack number of producers expected in
     *                the system
     */
    public ActiveMQMessageAudit(int auditDepth, final int maximumNumberOfProducersToTrack) {
        super(auditDepth, maximumNumberOfProducersToTrack);
    }
    
    @Override
    public boolean isDuplicate(String id) {
        synchronized (this) {
            return super.isDuplicate(id);
        }
    }

    @Override
    public boolean isDuplicate(final MessageId id) {
        synchronized (this) {
            return super.isDuplicate(id);
        }
    }

    @Override
    public void rollback(final  MessageId id) {
        synchronized (this) {
            super.rollback(id);
        }
    }
    
    @Override
    public boolean isInOrder(final String id) {
        synchronized (this) {
            return super.isInOrder(id);
        }
    }
    
    @Override
    public boolean isInOrder(final MessageId id) {
        synchronized (this) {
            return super.isInOrder(id);
        }
    }

    public void setMaximumNumberOfProducersToTrack(int maximumNumberOfProducersToTrack) {
        synchronized (this) {
            super.setMaximumNumberOfProducersToTrack(maximumNumberOfProducersToTrack);
        }
    }
}
