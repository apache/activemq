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

import org.apache.activemq.ActiveMQMessageAudit;
import org.apache.activemq.command.Message;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A strategy for choosing which destination is used for dead letter queue
 * messages.
 *
 */
public abstract class AbstractDeadLetterStrategy implements DeadLetterStrategy {
    private static final Logger LOG = LoggerFactory.getLogger(AbstractDeadLetterStrategy.class);
    private boolean processNonPersistent = false;
    private boolean processExpired = true;
    private boolean enableAudit = true;
    private final ActiveMQMessageAudit messageAudit = new ActiveMQMessageAudit();
    private long expiration;

    @Override
    public void rollback(Message message) {
        if (message != null && this.enableAudit) {
            messageAudit.rollback(message);
        }
    }

    @Override
    public boolean isSendToDeadLetterQueue(Message message) {
        boolean result = false;
        if (message != null) {
            result = true;
            if (enableAudit && messageAudit.isDuplicate(message)) {
                result = false;
                LOG.debug("Not adding duplicate to DLQ: {}, dest: {}", message.getMessageId(), message.getDestination());
            }
            if (!message.isPersistent() && !processNonPersistent) {
                result = false;
            }
            if (message.isExpired() && !processExpired) {
                result = false;
            }
        }
        return result;
    }

    /**
     * @return the processExpired
     */
    @Override
    public boolean isProcessExpired() {
        return this.processExpired;
    }

    /**
     * @param processExpired the processExpired to set
     */
    @Override
    public void setProcessExpired(boolean processExpired) {
        this.processExpired = processExpired;
    }

    /**
     * @return the processNonPersistent
     */
    @Override
    public boolean isProcessNonPersistent() {
        return this.processNonPersistent;
    }

    /**
     * @param processNonPersistent the processNonPersistent to set
     */
    @Override
    public void setProcessNonPersistent(boolean processNonPersistent) {
        this.processNonPersistent = processNonPersistent;
    }

    public boolean isEnableAudit() {
        return enableAudit;
    }

    public void setEnableAudit(boolean enableAudit) {
        this.enableAudit = enableAudit;
    }

    public long getExpiration() {
        return expiration;
    }

    public void setExpiration(long expiration) {
        this.expiration = expiration;
    }
}
