/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.activemq.openwire.v4;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activemq.openwire.*;
import org.apache.activemq.command.*;



/**
 * Marshalling code for Open Wire Format for MessageAckMarshaller
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * 
 */
public class MessageAckMarshaller extends BaseCommandMarshaller {

    /**
     * Return the type of Data Structure we marshal
     * @return short representation of the type data structure
     */
    public byte getDataStructureType() {
        return MessageAck.DATA_STRUCTURE_TYPE;
    }
    
    /**
     * @return a new object instance
     */
    public DataStructure createObject() {
        return new MessageAck();
    }

    /**
     * Un-marshal an object instance from the data input stream
     *
     * @param o the object to un-marshal
     * @param dataIn the data input stream to build the object from
     * @throws IOException
     */
    public void tightUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn, BooleanStream bs) throws IOException {
        super.tightUnmarshal(wireFormat, o, dataIn, bs);

        MessageAck info = (MessageAck)o;
        info.setDestination((org.apache.activemq.command.ActiveMQDestination) tightUnmarsalCachedObject(wireFormat, dataIn, bs));
        info.setTransactionId((org.apache.activemq.command.TransactionId) tightUnmarsalCachedObject(wireFormat, dataIn, bs));
        info.setConsumerId((org.apache.activemq.command.ConsumerId) tightUnmarsalCachedObject(wireFormat, dataIn, bs));
        info.setAckType(dataIn.readByte());
        info.setFirstMessageId((org.apache.activemq.command.MessageId) tightUnmarsalNestedObject(wireFormat, dataIn, bs));
        info.setLastMessageId((org.apache.activemq.command.MessageId) tightUnmarsalNestedObject(wireFormat, dataIn, bs));
        info.setMessageCount(dataIn.readInt());

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        MessageAck info = (MessageAck)o;

        int rc = super.tightMarshal1(wireFormat, o, bs);
        rc += tightMarshalCachedObject1(wireFormat, (DataStructure)info.getDestination(), bs);
        rc += tightMarshalCachedObject1(wireFormat, (DataStructure)info.getTransactionId(), bs);
        rc += tightMarshalCachedObject1(wireFormat, (DataStructure)info.getConsumerId(), bs);
        rc += tightMarshalNestedObject1(wireFormat, (DataStructure)info.getFirstMessageId(), bs);
        rc += tightMarshalNestedObject1(wireFormat, (DataStructure)info.getLastMessageId(), bs);

        return rc + 5;
    }

    /**
     * Write a object instance to data output stream
     *
     * @param o the instance to be marshaled
     * @param dataOut the output stream
     * @throws IOException thrown if an error occurs
     */
    public void tightMarshal2(OpenWireFormat wireFormat, Object o, DataOutput dataOut, BooleanStream bs) throws IOException {
        super.tightMarshal2(wireFormat, o, dataOut, bs);

        MessageAck info = (MessageAck)o;
        tightMarshalCachedObject2(wireFormat, (DataStructure)info.getDestination(), dataOut, bs);
        tightMarshalCachedObject2(wireFormat, (DataStructure)info.getTransactionId(), dataOut, bs);
        tightMarshalCachedObject2(wireFormat, (DataStructure)info.getConsumerId(), dataOut, bs);
        dataOut.writeByte(info.getAckType());
        tightMarshalNestedObject2(wireFormat, (DataStructure)info.getFirstMessageId(), dataOut, bs);
        tightMarshalNestedObject2(wireFormat, (DataStructure)info.getLastMessageId(), dataOut, bs);
        dataOut.writeInt(info.getMessageCount());

    }

    /**
     * Un-marshal an object instance from the data input stream
     *
     * @param o the object to un-marshal
     * @param dataIn the data input stream to build the object from
     * @throws IOException
     */
    public void looseUnmarshal(OpenWireFormat wireFormat, Object o, DataInput dataIn) throws IOException {
        super.looseUnmarshal(wireFormat, o, dataIn);

        MessageAck info = (MessageAck)o;
        info.setDestination((org.apache.activemq.command.ActiveMQDestination) looseUnmarsalCachedObject(wireFormat, dataIn));
        info.setTransactionId((org.apache.activemq.command.TransactionId) looseUnmarsalCachedObject(wireFormat, dataIn));
        info.setConsumerId((org.apache.activemq.command.ConsumerId) looseUnmarsalCachedObject(wireFormat, dataIn));
        info.setAckType(dataIn.readByte());
        info.setFirstMessageId((org.apache.activemq.command.MessageId) looseUnmarsalNestedObject(wireFormat, dataIn));
        info.setLastMessageId((org.apache.activemq.command.MessageId) looseUnmarsalNestedObject(wireFormat, dataIn));
        info.setMessageCount(dataIn.readInt());

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutput dataOut) throws IOException {

        MessageAck info = (MessageAck)o;

        super.looseMarshal(wireFormat, o, dataOut);
        looseMarshalCachedObject(wireFormat, (DataStructure)info.getDestination(), dataOut);
        looseMarshalCachedObject(wireFormat, (DataStructure)info.getTransactionId(), dataOut);
        looseMarshalCachedObject(wireFormat, (DataStructure)info.getConsumerId(), dataOut);
        dataOut.writeByte(info.getAckType());
        looseMarshalNestedObject(wireFormat, (DataStructure)info.getFirstMessageId(), dataOut);
        looseMarshalNestedObject(wireFormat, (DataStructure)info.getLastMessageId(), dataOut);
        dataOut.writeInt(info.getMessageCount());

    }
}
