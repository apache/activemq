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
package org.apache.activemq.command;

/**
 * @openwire:marshaller code="111"
 * @version $Revision: 1.11 $
 */
public class LocalTransactionId extends TransactionId implements Comparable<LocalTransactionId> {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.ACTIVEMQ_LOCAL_TRANSACTION_ID;

    protected ConnectionId connectionId;
    protected long value;

    private transient String transactionKey;
    private transient int hashCode;

    public LocalTransactionId() {
    }

    public LocalTransactionId(ConnectionId connectionId, long transactionId) {
        this.connectionId = connectionId;
        this.value = transactionId;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public boolean isXATransaction() {
        return false;
    }

    public boolean isLocalTransaction() {
        return true;
    }

    public String getTransactionKey() {
        if (transactionKey == null) {
            transactionKey = "TX:" + connectionId + ":" + value;
        }
        return transactionKey;
    }

    public String toString() {
        return getTransactionKey();
    }

    public int hashCode() {
        if (hashCode == 0) {
            hashCode = connectionId.hashCode() ^ (int)value;
        }
        return hashCode;
    }

    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || o.getClass() != LocalTransactionId.class) {
            return false;
        }
        LocalTransactionId tx = (LocalTransactionId)o;
        return value == tx.value && connectionId.equals(tx.connectionId);
    }

    /**
     * @param o
     * @return
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(LocalTransactionId o) {
        int result = connectionId.compareTo(o.connectionId);
        if (result == 0) {
            result = (int)(value - o.value);
        }
        return result;
    }

    /**
     * @openwire:property version=1
     */
    public long getValue() {
        return value;
    }

    public void setValue(long transactionId) {
        this.value = transactionId;
    }

    /**
     * @openwire:property version=1 cache=true
     */
    public ConnectionId getConnectionId() {
        return connectionId;
    }

    public void setConnectionId(ConnectionId connectionId) {
        this.connectionId = connectionId;
    }

}
