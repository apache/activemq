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

package org.apache.activemq.store.kahadb;

import org.apache.activemq.command.ConnectionId;
import org.apache.activemq.command.LocalTransactionId;
import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.XATransactionId;
import org.apache.activemq.protobuf.Buffer;
import org.apache.activemq.store.kahadb.data.KahaLocalTransactionId;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;
import org.apache.activemq.store.kahadb.data.KahaXATransactionId;

public class TransactionIdConversion {

    static KahaTransactionInfo convertToLocal(TransactionId tx) {
        KahaTransactionInfo rc = new KahaTransactionInfo();
        LocalTransactionId t = (LocalTransactionId) tx;
        KahaLocalTransactionId kahaTxId = new KahaLocalTransactionId();
        kahaTxId.setConnectionId(t.getConnectionId().getValue());
        kahaTxId.setTransacitonId(t.getValue());
        rc.setLocalTransacitonId(kahaTxId);
        return rc;
    }

    static KahaTransactionInfo convert(TransactionId txid) {
        if (txid == null) {
            return null;
        }
        KahaTransactionInfo rc;

        if (txid.isLocalTransaction()) {
            rc = convertToLocal(txid);
        } else {
            rc = new KahaTransactionInfo();
            XATransactionId t = (XATransactionId) txid;
            KahaXATransactionId kahaTxId = new KahaXATransactionId();
            kahaTxId.setBranchQualifier(new Buffer(t.getBranchQualifier()));
            kahaTxId.setGlobalTransactionId(new Buffer(t.getGlobalTransactionId()));
            kahaTxId.setFormatId(t.getFormatId());
            rc.setXaTransacitonId(kahaTxId);
        }
        return rc;
    }

    static TransactionId convert(KahaTransactionInfo transactionInfo) {
        if (transactionInfo.hasLocalTransacitonId()) {
            KahaLocalTransactionId tx = transactionInfo.getLocalTransacitonId();
            LocalTransactionId rc = new LocalTransactionId();
            rc.setConnectionId(new ConnectionId(tx.getConnectionId()));
            rc.setValue(tx.getTransacitonId());
            return rc;
        } else {
            KahaXATransactionId tx = transactionInfo.getXaTransacitonId();
            XATransactionId rc = new XATransactionId();
            rc.setBranchQualifier(tx.getBranchQualifier().toByteArray());
            rc.setGlobalTransactionId(tx.getGlobalTransactionId().toByteArray());
            rc.setFormatId(tx.getFormatId());
            return rc;
        }
    }
}