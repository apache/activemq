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
package org.apache.activemq.transport.amqp.client;

import org.apache.qpid.proton.amqp.Binary;

/**
 * Wrapper For Transaction state in identification
 */
public class AmqpTransactionId {

    public static final int DECLARE_MARKER = 1;
    public static final int ROLLBACK_MARKER = 2;
    public static final int COMMIT_MARKER = 3;

    private final String txId;
    private Binary remoteTxId;
    private int state = DECLARE_MARKER;

    public AmqpTransactionId(String txId) {
        this.txId = txId;
    }

    public boolean isDeclare() {
        return state == DECLARE_MARKER;
    }

    public boolean isCommit() {
        return state == COMMIT_MARKER;
    }

    public boolean isRollback() {
        return state == ROLLBACK_MARKER;
    }

    public void setState(int state) {
        this.state = state;
    }

    public String getTxId() {
        return txId;
    }

    public Binary getRemoteTxId() {
        return remoteTxId;
    }

    public void setRemoteTxId(Binary remoteTxId) {
        this.remoteTxId = remoteTxId;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((txId == null) ? 0 : txId.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }

        AmqpTransactionId other = (AmqpTransactionId) obj;
        if (txId == null) {
            if (other.txId != null) {
                return false;
            }
        } else if (!txId.equals(other.txId)) {
            return false;
        }

        return true;
    }
}
