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

public class Begin implements StompCommand {
    private StompWireFormat format;
    private static final HeaderParser parser = new HeaderParser();

    public Begin(StompWireFormat format) {
        this.format = format;
    }

    public CommandEnvelope build(String commandLine, DataInput in) throws IOException {
        Properties headers = parser.parse(in);
        while (in.readByte() != 0) {
        }

        TransactionInfo tx = new TransactionInfo();
        String user_tx_id = headers.getProperty(Stomp.Headers.TRANSACTION);
        if (!headers.containsKey(Stomp.Headers.TRANSACTION)) {
            throw new ProtocolException("Must specify the transaction you are beginning");
        }
        int tx_id = StompWireFormat.generateTransactionId();
        TransactionId transactionId = format.registerTransactionId(user_tx_id, tx_id);
        tx.setTransactionId(transactionId);
        tx.setType(TransactionInfo.BEGIN);
        return new CommandEnvelope(tx, headers);
    }
}
