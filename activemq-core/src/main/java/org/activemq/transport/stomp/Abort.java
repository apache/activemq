/**
 *
 * Copyright 2004 The Apache Software Foundation
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
package org.activemq.transport.stomp;

import org.activemq.command.TransactionId;
import org.activemq.command.TransactionInfo;

import java.io.DataInput;
import java.io.IOException;
import java.net.ProtocolException;
import java.util.Properties;

class Abort implements StompCommand {
    private StompWireFormat format;
    private static final HeaderParser parser = new HeaderParser();

    Abort(StompWireFormat format) {
        this.format = format;
    }

    public CommandEnvelope build(String commandLine, DataInput in) throws IOException {
        Properties headers = parser.parse(in);
        while (in.readByte() != 0) {
        }
        String user_tx_id = headers.getProperty(Stomp.Headers.TRANSACTION);

        if (!headers.containsKey(Stomp.Headers.TRANSACTION)) {
            throw new ProtocolException("Must specify the transaction you are aborting");
        }

        TransactionId txnId = format.getTransactionId(user_tx_id);
        TransactionInfo tx = new TransactionInfo();
        tx.setTransactionId(txnId);
        tx.setType(TransactionInfo.ROLLBACK);
        format.clearTransactionId(user_tx_id);
        return new CommandEnvelope(tx, headers);
    }
}
