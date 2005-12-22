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
package org.activemq.openwire.v1;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.activemq.openwire.*;
import org.activemq.command.*;


/**
 * Marshalling code for Open Wire Format for MessageAck
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version $Revision$
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
    public void unmarshal(OpenWireFormat wireFormat, Object o, DataInputStream dataIn, BooleanStream bs) throws IOException {
        super.unmarshal(wireFormat, o, dataIn, bs);

        MessageAck info = (MessageAck)o;
        info.setDestination((org.activemq.command.ActiveMQDestination) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setTransactionId((org.activemq.command.TransactionId) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setConsumerId((org.activemq.command.ConsumerId) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setAckType(dataIn.readByte());
        info.setFirstMessageId((org.activemq.command.MessageId) unmarsalNestedObject(wireFormat, dataIn, bs));
        info.setLastMessageId((org.activemq.command.MessageId) unmarsalNestedObject(wireFormat, dataIn, bs));
        info.setMessageCount(dataIn.readInt());

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int marshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        MessageAck info = (MessageAck)o;

        int rc = super.marshal1(wireFormat, o, bs);
        rc += marshal1CachedObject(wireFormat, info.getDestination(), bs);
        rc += marshal1CachedObject(wireFormat, info.getTransactionId(), bs);
        rc += marshal1CachedObject(wireFormat, info.getConsumerId(), bs);
        
        rc += marshal1NestedObject(wireFormat, info.getFirstMessageId(), bs);
        rc += marshal1NestedObject(wireFormat, info.getLastMessageId(), bs);
        

        return rc+5;
    }

    /**
     * Write a object instance to data output stream
     *
     * @param o the instance to be marshaled
     * @param dataOut the output stream
     * @throws IOException thrown if an error occurs
     */
    public void marshal2(OpenWireFormat wireFormat, Object o, DataOutputStream dataOut, BooleanStream bs) throws IOException {
        super.marshal2(wireFormat, o, dataOut, bs);

        MessageAck info = (MessageAck)o;
        marshal2CachedObject(wireFormat, info.getDestination(), dataOut, bs);
        marshal2CachedObject(wireFormat, info.getTransactionId(), dataOut, bs);
        marshal2CachedObject(wireFormat, info.getConsumerId(), dataOut, bs);
        dataOut.writeByte(info.getAckType());
        marshal2NestedObject(wireFormat, info.getFirstMessageId(), dataOut, bs);
        marshal2NestedObject(wireFormat, info.getLastMessageId(), dataOut, bs);
        dataOut.writeInt(info.getMessageCount());

    }
}
