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

package org.apache.activemq.openwire.v12;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.activemq.openwire.*;
import org.apache.activemq.command.*;



/**
 * Marshalling code for Open Wire Format for XATransactionIdMarshaller
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * 
 */
public class XATransactionIdMarshaller extends TransactionIdMarshaller {

    /**
     * Return the type of Data Structure we marshal
     * @return short representation of the type data structure
     */
    public byte getDataStructureType() {
        return XATransactionId.DATA_STRUCTURE_TYPE;
    }
    
    /**
     * @return a new object instance
     */
    public DataStructure createObject() {
        return new XATransactionId();
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

        XATransactionId info = (XATransactionId)o;
        info.setFormatId(dataIn.readInt());
        info.setGlobalTransactionId(tightUnmarshalByteArray(dataIn, bs));
        info.setBranchQualifier(tightUnmarshalByteArray(dataIn, bs));

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        XATransactionId info = (XATransactionId)o;

        int rc = super.tightMarshal1(wireFormat, o, bs);
        rc += tightMarshalByteArray1(info.getGlobalTransactionId(), bs);
        rc += tightMarshalByteArray1(info.getBranchQualifier(), bs);

        return rc + 4;
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

        XATransactionId info = (XATransactionId)o;
        dataOut.writeInt(info.getFormatId());
        tightMarshalByteArray2(info.getGlobalTransactionId(), dataOut, bs);
        tightMarshalByteArray2(info.getBranchQualifier(), dataOut, bs);

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

        XATransactionId info = (XATransactionId)o;
        info.setFormatId(dataIn.readInt());
        info.setGlobalTransactionId(looseUnmarshalByteArray(dataIn));
        info.setBranchQualifier(looseUnmarshalByteArray(dataIn));

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutput dataOut) throws IOException {

        XATransactionId info = (XATransactionId)o;

        super.looseMarshal(wireFormat, o, dataOut);
        dataOut.writeInt(info.getFormatId());
        looseMarshalByteArray(wireFormat, info.getGlobalTransactionId(), dataOut);
        looseMarshalByteArray(wireFormat, info.getBranchQualifier(), dataOut);

    }
}
