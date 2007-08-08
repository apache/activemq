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

package org.apache.activemq.store.amq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.activemq.broker.ConnectionContext;
import org.apache.activemq.command.ActiveMQDestination;
import org.apache.activemq.command.JournalTopicAck;
import org.apache.activemq.command.Message;
import org.apache.activemq.command.MessageAck;
import org.apache.activemq.kaha.impl.async.Location;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;

/**
 */
public class AMQTxOperation {

    public static final byte ADD_OPERATION_TYPE = 0;
    public static final byte REMOVE_OPERATION_TYPE = 1;
    public static final byte ACK_OPERATION_TYPE = 3;
    private byte operationType;
    private ActiveMQDestination destination;
    private Object data;
    private Location location;

    public AMQTxOperation() {
    }

    public AMQTxOperation(byte operationType, ActiveMQDestination destination, Object data, Location location) {
        this.operationType = operationType;
        this.destination = destination;
        this.data = data;
        this.location = location;

    }

    /**
     * @return the data
     */
    public Object getData() {
        return this.data;
    }

    /**
     * @param data the data to set
     */
    public void setData(Object data) {
        this.data = data;
    }

    /**
     * @return the location
     */
    public Location getLocation() {
        return this.location;
    }

    /**
     * @param location the location to set
     */
    public void setLocation(Location location) {
        this.location = location;
    }

    /**
     * @return the operationType
     */
    public byte getOperationType() {
        return this.operationType;
    }

    /**
     * @param operationType the operationType to set
     */
    public void setOperationType(byte operationType) {
        this.operationType = operationType;
    }

    public boolean replay(AMQPersistenceAdapter adapter, ConnectionContext context) throws IOException {
        boolean result = false;
        AMQMessageStore store = (AMQMessageStore)adapter.createMessageStore(destination);
        if (operationType == ADD_OPERATION_TYPE) {
            result = store.replayAddMessage(context, (Message)data, location);
        } else if (operationType == REMOVE_OPERATION_TYPE) {
            result = store.replayRemoveMessage(context, (MessageAck)data);
        } else {
            JournalTopicAck ack = (JournalTopicAck)data;
            result = ((AMQTopicMessageStore)store).replayAcknowledge(context, ack.getClientId(), ack
                .getSubscritionName(), ack.getMessageId());
        }
        return result;
    }

    public void writeExternal(WireFormat wireFormat, DataOutput dos) throws IOException {
        location.writeExternal(dos);
        ByteSequence packet = wireFormat.marshal(getData());
        dos.writeInt(packet.length);
        dos.write(packet.data, packet.offset, packet.length);
        packet = wireFormat.marshal(destination);
        dos.writeInt(packet.length);
        dos.write(packet.data, packet.offset, packet.length);
    }

    public void readExternal(WireFormat wireFormat, DataInput dis) throws IOException {
        this.location = new Location();
        this.location.readExternal(dis);
        int size = dis.readInt();
        byte[] data = new byte[size];
        dis.readFully(data);
        setData(wireFormat.unmarshal(new ByteSequence(data)));
        size = dis.readInt();
        data = new byte[size];
        dis.readFully(data);
        this.destination = (ActiveMQDestination)wireFormat.unmarshal(new ByteSequence(data));
    }
}
