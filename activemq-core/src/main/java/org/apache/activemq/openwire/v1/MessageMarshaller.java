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

package org.apache.activemq.openwire.v1;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.openwire.*;
import org.apache.activemq.command.*;


/**
 * Marshalling code for Open Wire Format for MessageMarshaller
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version $Revision$
 */
public abstract class MessageMarshaller extends BaseCommandMarshaller {

    /**
     * Un-marshal an object instance from the data input stream
     *
     * @param o the object to un-marshal
     * @param dataIn the data input stream to build the object from
     * @throws IOException
     */
    public void unmarshal(OpenWireFormat wireFormat, Object o, DataInputStream dataIn, BooleanStream bs) throws IOException {
        super.unmarshal(wireFormat, o, dataIn, bs);

        Message info = (Message)o;

        info.beforeUnmarshall(wireFormat);
        
        info.setProducerId((ProducerId) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setDestination((ActiveMQDestination) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setTransactionId((TransactionId) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setOriginalDestination((ActiveMQDestination) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setMessageId((MessageId) unmarsalNestedObject(wireFormat, dataIn, bs));
        info.setOriginalTransactionId((TransactionId) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setGroupID(readString(dataIn, bs));
        info.setGroupSequence(dataIn.readInt());
        info.setCorrelationId(readString(dataIn, bs));
        info.setPersistent(bs.readBoolean());
        info.setExpiration(unmarshalLong(wireFormat, dataIn, bs));
        info.setPriority(dataIn.readByte());
        info.setReplyTo((ActiveMQDestination) unmarsalNestedObject(wireFormat, dataIn, bs));
        info.setTimestamp(unmarshalLong(wireFormat, dataIn, bs));
        info.setType(readString(dataIn, bs));
        if( bs.readBoolean() ) {
            int size = dataIn.readInt();
            byte data[] = new byte[size];
            dataIn.readFully(data);
            info.setContent(new org.activeio.ByteSequence(data,0,size));
            } else {
            info.setContent(null);
        }
        if( bs.readBoolean() ) {
            int size = dataIn.readInt();
            byte data[] = new byte[size];
            dataIn.readFully(data);
            info.setMarshalledProperties(new org.activeio.ByteSequence(data,0,size));
            } else {
            info.setMarshalledProperties(null);
        }
        info.setDataStructure((DataStructure) unmarsalNestedObject(wireFormat, dataIn, bs));
        info.setTargetConsumerId((ConsumerId) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setCompressed(bs.readBoolean());
        info.setRedeliveryCounter(dataIn.readInt());

        if (bs.readBoolean()) {
            short size = dataIn.readShort();
            BrokerId value[] = new BrokerId[size];
            for( int i=0; i < size; i++ ) {
                value[i] = (BrokerId) unmarsalNestedObject(wireFormat,dataIn, bs);
            }
            info.setBrokerPath(value);
        }
        else {
            info.setBrokerPath(null);
        }
        info.setArrival(unmarshalLong(wireFormat, dataIn, bs));
        info.setUserID(readString(dataIn, bs));
        info.setRecievedByDFBridge(bs.readBoolean());

        info.afterUnmarshall(wireFormat);

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int marshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        Message info = (Message)o;

        info.beforeMarshall(wireFormat);

        int rc = super.marshal1(wireFormat, o, bs);
        rc += marshal1CachedObject(wireFormat, info.getProducerId(), bs);
        rc += marshal1CachedObject(wireFormat, info.getDestination(), bs);
        rc += marshal1CachedObject(wireFormat, info.getTransactionId(), bs);
        rc += marshal1CachedObject(wireFormat, info.getOriginalDestination(), bs);
        rc += marshal1NestedObject(wireFormat, info.getMessageId(), bs);
        rc += marshal1CachedObject(wireFormat, info.getOriginalTransactionId(), bs);
        rc += writeString(info.getGroupID(), bs);
                rc += writeString(info.getCorrelationId(), bs);
        bs.writeBoolean(info.isPersistent());
        rc+=marshal1Long(wireFormat, info.getExpiration(), bs);
                rc += marshal1NestedObject(wireFormat, info.getReplyTo(), bs);
        rc+=marshal1Long(wireFormat, info.getTimestamp(), bs);
        rc += writeString(info.getType(), bs);
        bs.writeBoolean(info.getContent()!=null);
        rc += info.getContent()==null ? 0 : info.getContent().getLength()+4;
        bs.writeBoolean(info.getMarshalledProperties()!=null);
        rc += info.getMarshalledProperties()==null ? 0 : info.getMarshalledProperties().getLength()+4;
        rc += marshal1NestedObject(wireFormat, info.getDataStructure(), bs);
        rc += marshal1CachedObject(wireFormat, info.getTargetConsumerId(), bs);
        bs.writeBoolean(info.isCompressed());
                rc += marshalObjectArray(wireFormat, info.getBrokerPath(), bs);
        rc+=marshal1Long(wireFormat, info.getArrival(), bs);
        rc += writeString(info.getUserID(), bs);
        bs.writeBoolean(info.isRecievedByDFBridge());

        return rc + 3;
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

        Message info = (Message)o;
        marshal2CachedObject(wireFormat, info.getProducerId(), dataOut, bs);
        marshal2CachedObject(wireFormat, info.getDestination(), dataOut, bs);
        marshal2CachedObject(wireFormat, info.getTransactionId(), dataOut, bs);
        marshal2CachedObject(wireFormat, info.getOriginalDestination(), dataOut, bs);
        marshal2NestedObject(wireFormat, info.getMessageId(), dataOut, bs);
        marshal2CachedObject(wireFormat, info.getOriginalTransactionId(), dataOut, bs);
        writeString(info.getGroupID(), dataOut, bs);
        dataOut.writeInt(info.getGroupSequence());
        writeString(info.getCorrelationId(), dataOut, bs);
        bs.readBoolean();
        marshal2Long(wireFormat, info.getExpiration(), dataOut, bs);
        dataOut.writeByte(info.getPriority());
        marshal2NestedObject(wireFormat, info.getReplyTo(), dataOut, bs);
        marshal2Long(wireFormat, info.getTimestamp(), dataOut, bs);
        writeString(info.getType(), dataOut, bs);
        if(bs.readBoolean()) {
           org.activeio.ByteSequence data = info.getContent();
           dataOut.writeInt(data.getLength());
           dataOut.write(data.getData(), data.getOffset(), data.getLength());
        }
        if(bs.readBoolean()) {
           org.activeio.ByteSequence data = info.getMarshalledProperties();
           dataOut.writeInt(data.getLength());
           dataOut.write(data.getData(), data.getOffset(), data.getLength());
        }
        marshal2NestedObject(wireFormat, info.getDataStructure(), dataOut, bs);
        marshal2CachedObject(wireFormat, info.getTargetConsumerId(), dataOut, bs);
        bs.readBoolean();
        dataOut.writeInt(info.getRedeliveryCounter());
        marshalObjectArray(wireFormat, info.getBrokerPath(), dataOut, bs);
        marshal2Long(wireFormat, info.getArrival(), dataOut, bs);
        writeString(info.getUserID(), dataOut, bs);
        bs.readBoolean();

        info.afterMarshall(wireFormat);

    }
}
