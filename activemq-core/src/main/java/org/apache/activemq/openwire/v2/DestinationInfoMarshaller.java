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

package org.apache.activemq.openwire.v2;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activemq.openwire.*;
import org.apache.activemq.command.*;



/**
 * Marshalling code for Open Wire Format for DestinationInfoMarshaller
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version $Revision$
 */
public class DestinationInfoMarshaller extends BaseCommandMarshaller {

    /**
     * Return the type of Data Structure we marshal
     * @return short representation of the type data structure
     */
    public byte getDataStructureType() {
        return DestinationInfo.DATA_STRUCTURE_TYPE;
    }
    
    /**
     * @return a new object instance
     */
    public DataStructure createObject() {
        return new DestinationInfo();
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

        DestinationInfo info = (DestinationInfo)o;
        info.setConnectionId((org.apache.activemq.command.ConnectionId) tightUnmarsalCachedObject(wireFormat, dataIn, bs));
        info.setDestination((org.apache.activemq.command.ActiveMQDestination) tightUnmarsalCachedObject(wireFormat, dataIn, bs));
        info.setOperationType(dataIn.readByte());
        info.setTimeout(tightUnmarshalLong(wireFormat, dataIn, bs));

        if (bs.readBoolean()) {
            short size = dataIn.readShort();
            org.apache.activemq.command.BrokerId value[] = new org.apache.activemq.command.BrokerId[size];
            for( int i=0; i < size; i++ ) {
                value[i] = (org.apache.activemq.command.BrokerId) tightUnmarsalNestedObject(wireFormat,dataIn, bs);
            }
            info.setBrokerPath(value);
        }
        else {
            info.setBrokerPath(null);
        }

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        DestinationInfo info = (DestinationInfo)o;

        int rc = super.tightMarshal1(wireFormat, o, bs);
        rc += tightMarshalCachedObject1(wireFormat, (DataStructure)info.getConnectionId(), bs);
        rc += tightMarshalCachedObject1(wireFormat, (DataStructure)info.getDestination(), bs);
        rc+=tightMarshalLong1(wireFormat, info.getTimeout(), bs);
        rc += tightMarshalObjectArray1(wireFormat, info.getBrokerPath(), bs);

        return rc + 1;
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

        DestinationInfo info = (DestinationInfo)o;
        tightMarshalCachedObject2(wireFormat, (DataStructure)info.getConnectionId(), dataOut, bs);
        tightMarshalCachedObject2(wireFormat, (DataStructure)info.getDestination(), dataOut, bs);
        dataOut.writeByte(info.getOperationType());
        tightMarshalLong2(wireFormat, info.getTimeout(), dataOut, bs);
        tightMarshalObjectArray2(wireFormat, info.getBrokerPath(), dataOut, bs);

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

        DestinationInfo info = (DestinationInfo)o;
        info.setConnectionId((org.apache.activemq.command.ConnectionId) looseUnmarsalCachedObject(wireFormat, dataIn));
        info.setDestination((org.apache.activemq.command.ActiveMQDestination) looseUnmarsalCachedObject(wireFormat, dataIn));
        info.setOperationType(dataIn.readByte());
        info.setTimeout(looseUnmarshalLong(wireFormat, dataIn));

        if (dataIn.readBoolean()) {
            short size = dataIn.readShort();
            org.apache.activemq.command.BrokerId value[] = new org.apache.activemq.command.BrokerId[size];
            for( int i=0; i < size; i++ ) {
                value[i] = (org.apache.activemq.command.BrokerId) looseUnmarsalNestedObject(wireFormat,dataIn);
            }
            info.setBrokerPath(value);
        }
        else {
            info.setBrokerPath(null);
        }

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutput dataOut) throws IOException {

        DestinationInfo info = (DestinationInfo)o;

        super.looseMarshal(wireFormat, o, dataOut);
        looseMarshalCachedObject(wireFormat, (DataStructure)info.getConnectionId(), dataOut);
        looseMarshalCachedObject(wireFormat, (DataStructure)info.getDestination(), dataOut);
        dataOut.writeByte(info.getOperationType());
        looseMarshalLong(wireFormat, info.getTimeout(), dataOut);
        looseMarshalObjectArray(wireFormat, info.getBrokerPath(), dataOut);

    }
}
