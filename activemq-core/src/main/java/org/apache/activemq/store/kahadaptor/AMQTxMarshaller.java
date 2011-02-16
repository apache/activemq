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
package org.apache.activemq.store.kahadaptor;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.activemq.kaha.Marshaller;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.store.amq.AMQTx;
import org.apache.activemq.store.amq.AMQTxOperation;
import org.apache.activemq.wireformat.WireFormat;

/**
 * Marshall an AMQTx
 * 
 * 
 */
public class AMQTxMarshaller implements Marshaller<AMQTx> {

    private WireFormat wireFormat;

    public AMQTxMarshaller(WireFormat wireFormat) {
        this.wireFormat = wireFormat;
    }

    public AMQTx readPayload(DataInput dataIn) throws IOException {
        Location location = new Location();
        location.readExternal(dataIn);
        AMQTx result = new AMQTx(location);
        int size = dataIn.readInt();
        for (int i = 0; i < size; i++) {
            AMQTxOperation op = new AMQTxOperation();
            op.readExternal(wireFormat, dataIn);
            result.getOperations().add(op);
        }
        return result;
    }

    public void writePayload(AMQTx amqtx, DataOutput dataOut) throws IOException {
        amqtx.getLocation().writeExternal(dataOut);
        List<AMQTxOperation> list = amqtx.getOperations();
        List<AMQTxOperation> ops = new ArrayList<AMQTxOperation>();
        
        for (AMQTxOperation op : list) {
            if (op.getOperationType() == op.ADD_OPERATION_TYPE) {
                ops.add(op);
            }
        }
        dataOut.writeInt(ops.size());
        for (AMQTxOperation op : ops) {
            op.writeExternal(wireFormat, dataOut);
        }
    }
}
