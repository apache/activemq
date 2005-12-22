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
 * Marshalling code for Open Wire Format for JournalTopicAck
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version $Revision$
 */
public class JournalTopicAckMarshaller extends org.activemq.openwire.DataStreamMarshaller {

    /**
     * Return the type of Data Structure we marshal
     * @return short representation of the type data structure
     */
    public byte getDataStructureType() {
        return JournalTopicAck.DATA_STRUCTURE_TYPE;
    }
    
    /**
     * @return a new object instance
     */
    public DataStructure createObject() {
        return new JournalTopicAck();
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

        JournalTopicAck info = (JournalTopicAck)o;
        info.setDestination((org.activemq.command.ActiveMQDestination) unmarsalNestedObject(wireFormat, dataIn, bs));
        info.setMessageId((org.activemq.command.MessageId) unmarsalNestedObject(wireFormat, dataIn, bs));
        info.setMessageSequenceId(unmarshalLong(wireFormat, dataIn, bs));
        info.setSubscritionName(readString(dataIn, bs));
        info.setClientId(readString(dataIn, bs));
        info.setTransactionId((org.activemq.command.TransactionId) unmarsalNestedObject(wireFormat, dataIn, bs));

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int marshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        JournalTopicAck info = (JournalTopicAck)o;

        int rc = super.marshal1(wireFormat, o, bs);
        rc += marshal1NestedObject(wireFormat, info.getDestination(), bs);
        rc += marshal1NestedObject(wireFormat, info.getMessageId(), bs);
        rc+=marshal1Long(wireFormat, info.getMessageSequenceId(), bs);
        rc += writeString(info.getSubscritionName(), bs);
        rc += writeString(info.getClientId(), bs);
        rc += marshal1NestedObject(wireFormat, info.getTransactionId(), bs);

        return rc+0;
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

        JournalTopicAck info = (JournalTopicAck)o;
        marshal2NestedObject(wireFormat, info.getDestination(), dataOut, bs);
        marshal2NestedObject(wireFormat, info.getMessageId(), dataOut, bs);
        marshal2Long(wireFormat, info.getMessageSequenceId(), dataOut, bs);
        writeString(info.getSubscritionName(), dataOut, bs);
        writeString(info.getClientId(), dataOut, bs);
        marshal2NestedObject(wireFormat, info.getTransactionId(), dataOut, bs);

    }
}
