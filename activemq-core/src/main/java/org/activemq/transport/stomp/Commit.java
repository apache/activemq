/*
 * Copyright (c) 2005 Your Corporation. All Rights Reserved.
 */
package org.activemq.transport.stomp;

import org.activemq.command.TransactionId;
import org.activemq.command.TransactionInfo;

import java.io.DataInput;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.Properties;

class Commit implements StompCommand {
    private StompWireFormat format;
    private static final HeaderParser parser = new HeaderParser();

    Commit(StompWireFormat format) {
        this.format = format;
    }

    public CommandEnvelope build(String commandLine, DataInput in) throws IOException {
        Properties headers = parser.parse(in);
        while (in.readByte() != 0) {
        }

        String user_tx_id = headers.getProperty(Stomp.Headers.TRANSACTION);

        if (!headers.containsKey(Stomp.Headers.TRANSACTION)) {
            throw new ProtocolException("Must specify the transaction you are committing");
        }

        TransactionId tx_id = format.getTransactionId(user_tx_id);
        TransactionInfo tx = new TransactionInfo();
        tx.setTransactionId(tx_id);
        tx.setType(TransactionInfo.COMMIT_ONE_PHASE);
        format.clearTransactionId(user_tx_id);
        return new CommandEnvelope(tx, headers);
    }
}
