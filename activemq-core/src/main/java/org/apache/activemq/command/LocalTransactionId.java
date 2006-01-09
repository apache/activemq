/**
 *
 * Copyright 2005-2006 The Apache Software Foundation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
public class LocalTransactionId extends TransactionId {

    public static final byte DATA_STRUCTURE_TYPE=CommandTypes.ACTIVEMQ_LOCAL_TRANSACTION_ID;

    protected ConnectionId connectionId;
    protected long transactionId;

    private transient String transactionKey;
    private transient int hashCode;

    public LocalTransactionId() {        
    }
    
    public LocalTransactionId(ConnectionId connectionId, long transactionId) {
        this.connectionId=connectionId;
        this.transactionId=transactionId;
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
        if( transactionKey==null ) {
            transactionKey = "TX:"+connectionId+":"+transactionId;
        }
        return transactionKey;
    }
    
    public String toString() {
        return getTransactionKey();
    }

    public int hashCode() {
        if( hashCode == 0 ) {
            hashCode = connectionId.hashCode() ^ (int)transactionId;
        }
        return hashCode;
    }
    
    public boolean equals(Object o) {
        if( this == o )
            return true;
        if( o == null || o.getClass()!=LocalTransactionId.class )
            return false;
        LocalTransactionId tx = (LocalTransactionId) o;
        return transactionId==tx.transactionId 
                && connectionId.equals(tx.connectionId);
    }
    
    /**
     * @openwire:property version=1
     */
    public long getTransactionId() {
        return transactionId;
    }
    public void setTransactionId(long transactionId) {
        this.transactionId = transactionId;
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
