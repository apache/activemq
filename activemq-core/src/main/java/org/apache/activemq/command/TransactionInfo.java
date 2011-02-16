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

import java.io.IOException;

import org.apache.activemq.state.CommandVisitor;

/**
 * 
 * @openwire:marshaller code="7"
 * 
 */
public class TransactionInfo extends BaseCommand {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.TRANSACTION_INFO;

    public static final byte BEGIN = 0;
    public static final byte PREPARE = 1;
    public static final byte COMMIT_ONE_PHASE = 2;
    public static final byte COMMIT_TWO_PHASE = 3;
    public static final byte ROLLBACK = 4;
    public static final byte RECOVER = 5;
    public static final byte FORGET = 6;
    public static final byte END = 7;

    protected byte type;
    protected ConnectionId connectionId;
    protected TransactionId transactionId;

    public TransactionInfo() {
    }

    public TransactionInfo(ConnectionId connectionId, TransactionId transactionId, byte type) {
        this.connectionId = connectionId;
        this.transactionId = transactionId;
        this.type = type;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
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

    /**
     * @openwire:property version=1 cache=true
     */
    public TransactionId getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(TransactionId transactionId) {
        this.transactionId = transactionId;
    }

    /**
     * @openwire:property version=1
     */
    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public Response visit(CommandVisitor visitor) throws Exception {
        switch (type) {
        case TransactionInfo.BEGIN:
            return visitor.processBeginTransaction(this);
        case TransactionInfo.END:
            return visitor.processEndTransaction(this);
        case TransactionInfo.PREPARE:
            return visitor.processPrepareTransaction(this);
        case TransactionInfo.COMMIT_ONE_PHASE:
            return visitor.processCommitTransactionOnePhase(this);
        case TransactionInfo.COMMIT_TWO_PHASE:
            return visitor.processCommitTransactionTwoPhase(this);
        case TransactionInfo.ROLLBACK:
            return visitor.processRollbackTransaction(this);
        case TransactionInfo.RECOVER:
            return visitor.processRecoverTransactions(this);
        case TransactionInfo.FORGET:
            return visitor.processForgetTransaction(this);
        default:
            throw new IOException("Transaction info type unknown: " + type);
        }
    }

}
