package org.apache.activemq.store.kahadb;

import org.apache.activemq.command.TransactionId;
import org.apache.activemq.store.kahadb.data.KahaTransactionInfo;

public interface TransactionIdTransformer {
    KahaTransactionInfo transform(TransactionId txid);
}
