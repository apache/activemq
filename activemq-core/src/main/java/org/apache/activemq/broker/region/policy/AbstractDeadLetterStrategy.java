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

import org.apache.activemq.command.Message;

/**
 * A strategy for choosing which destination is used for dead letter queue
 * messages.
 * 
 * @version $Revision: 426366 $
 */
public abstract class AbstractDeadLetterStrategy implements DeadLetterStrategy {
    private boolean processNonPersistent = true;
    private boolean processExpired = true;

    public boolean isSendToDeadLetterQueue(Message message) {
        boolean result = false;
        if (message != null) {
            result = true;
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
    public boolean isProcessExpired() {
        return this.processExpired;
    }

    /**
     * @param processExpired the processExpired to set
     */
    public void setProcessExpired(boolean processExpired) {
        this.processExpired = processExpired;
    }

    /**
     * @return the processNonPersistent
     */
    public boolean isProcessNonPersistent() {
        return this.processNonPersistent;
    }

    /**
     * @param processNonPersistent the processNonPersistent to set
     */
    public void setProcessNonPersistent(boolean processNonPersistent) {
        this.processNonPersistent = processNonPersistent;
    }

}
