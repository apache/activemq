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
 * Marshalling code for Open Wire Format for JournalTopicAckMarshaller
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version $Revision$
 */
public class JournalTopicAckMarshaller extends BaseDataStreamMarshaller {

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
    public void tightUnmarshal(OpenWireFormat wireFormat, Object o, DataInputStream dataIn, BooleanStream bs) throws IOException {
        super.tightUnmarshal(wireFormat, o, dataIn, bs);

        JournalTopicAck info = (JournalTopicAck)o;
        info.setDestination((ActiveMQDestination) tightUnmarsalNestedObject(wireFormat, dataIn, bs));
        info.setMessageId((MessageId) tightUnmarsalNestedObject(wireFormat, dataIn, bs));
        info.setMessageSequenceId(tightUnmarshalLong(wireFormat, dataIn, bs));
        info.setSubscritionName(tightUnmarshalString(dataIn, bs));
        info.setClientId(tightUnmarshalString(dataIn, bs));
        info.setTransactionId((TransactionId) tightUnmarsalNestedObject(wireFormat, dataIn, bs));

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        JournalTopicAck info = (JournalTopicAck)o;

        int rc = super.tightMarshal1(wireFormat, o, bs);
    rc += tightMarshalNestedObject1(wireFormat, info.getDestination(), bs);
    rc += tightMarshalNestedObject1(wireFormat, info.getMessageId(), bs);
    rc+=tightMarshalLong1(wireFormat, info.getMessageSequenceId(), bs);
    rc += tightMarshalString1(info.getSubscritionName(), bs);
    rc += tightMarshalString1(info.getClientId(), bs);
    rc += tightMarshalNestedObject1(wireFormat, info.getTransactionId(), bs);

        return rc + 0;
    }

    /**
     * Write a object instance to data output stream
     *
     * @param o the instance to be marshaled
     * @param dataOut the output stream
     * @throws IOException thrown if an error occurs
     */
    public void tightMarshal2(OpenWireFormat wireFormat, Object o, DataOutputStream dataOut, BooleanStream bs) throws IOException {
        super.tightMarshal2(wireFormat, o, dataOut, bs);

        JournalTopicAck info = (JournalTopicAck)o;
    tightMarshalNestedObject2(wireFormat, info.getDestination(), dataOut, bs);
    tightMarshalNestedObject2(wireFormat, info.getMessageId(), dataOut, bs);
    tightMarshalLong2(wireFormat, info.getMessageSequenceId(), dataOut, bs);
    tightMarshalString2(info.getSubscritionName(), dataOut, bs);
    tightMarshalString2(info.getClientId(), dataOut, bs);
    tightMarshalNestedObject2(wireFormat, info.getTransactionId(), dataOut, bs);

    }

    /**
     * Un-marshal an object instance from the data input stream
     *
     * @param o the object to un-marshal
     * @param dataIn the data input stream to build the object from
     * @throws IOException
     */
    public void looseUnmarshal(OpenWireFormat wireFormat, Object o, DataInputStream dataIn) throws IOException {
        super.looseUnmarshal(wireFormat, o, dataIn);

        JournalTopicAck info = (JournalTopicAck)o;
        info.setDestination((ActiveMQDestination) looseUnmarsalNestedObject(wireFormat, dataIn));
        info.setMessageId((MessageId) looseUnmarsalNestedObject(wireFormat, dataIn));
        info.setMessageSequenceId(looseUnmarshalLong(wireFormat, dataIn));
        info.setSubscritionName(looseUnmarshalString(dataIn));
        info.setClientId(looseUnmarshalString(dataIn));
        info.setTransactionId((TransactionId) looseUnmarsalNestedObject(wireFormat, dataIn));

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutputStream dataOut) throws IOException {

        JournalTopicAck info = (JournalTopicAck)o;

        super.looseMarshal(wireFormat, o, dataOut);
    looseMarshalNestedObject(wireFormat, info.getDestination(), dataOut);
    looseMarshalNestedObject(wireFormat, info.getMessageId(), dataOut);
    looseMarshalLong(wireFormat, info.getMessageSequenceId(), dataOut);
    looseMarshalString(info.getSubscritionName(), dataOut);
    looseMarshalString(info.getClientId(), dataOut);
    looseMarshalNestedObject(wireFormat, info.getTransactionId(), dataOut);

    }
}
