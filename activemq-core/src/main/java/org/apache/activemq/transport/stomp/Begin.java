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
package org.apache.activemq.transport.stomp;

import org.apache.activemq.command.TransactionId;
import org.apache.activemq.command.TransactionInfo;

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
