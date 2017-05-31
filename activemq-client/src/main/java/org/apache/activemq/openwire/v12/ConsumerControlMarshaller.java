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
 * Marshalling code for Open Wire Format for ConsumerControlMarshaller
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * 
 */
public class ConsumerControlMarshaller extends BaseCommandMarshaller {

    /**
     * Return the type of Data Structure we marshal
     * @return short representation of the type data structure
     */
    public byte getDataStructureType() {
        return ConsumerControl.DATA_STRUCTURE_TYPE;
    }
    
    /**
     * @return a new object instance
     */
    public DataStructure createObject() {
        return new ConsumerControl();
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

        ConsumerControl info = (ConsumerControl)o;
        info.setDestination((org.apache.activemq.command.ActiveMQDestination) tightUnmarsalNestedObject(wireFormat, dataIn, bs));
        info.setClose(bs.readBoolean());
        info.setConsumerId((org.apache.activemq.command.ConsumerId) tightUnmarsalNestedObject(wireFormat, dataIn, bs));
        info.setPrefetch(dataIn.readInt());
        info.setFlush(bs.readBoolean());
        info.setStart(bs.readBoolean());
        info.setStop(bs.readBoolean());

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int tightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        ConsumerControl info = (ConsumerControl)o;

        int rc = super.tightMarshal1(wireFormat, o, bs);
        rc += tightMarshalNestedObject1(wireFormat, (DataStructure)info.getDestination(), bs);
        bs.writeBoolean(info.isClose());
        rc += tightMarshalNestedObject1(wireFormat, (DataStructure)info.getConsumerId(), bs);
        bs.writeBoolean(info.isFlush());
        bs.writeBoolean(info.isStart());
        bs.writeBoolean(info.isStop());

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

        ConsumerControl info = (ConsumerControl)o;
        tightMarshalNestedObject2(wireFormat, (DataStructure)info.getDestination(), dataOut, bs);
        bs.readBoolean();
        tightMarshalNestedObject2(wireFormat, (DataStructure)info.getConsumerId(), dataOut, bs);
        dataOut.writeInt(info.getPrefetch());
        bs.readBoolean();
        bs.readBoolean();
        bs.readBoolean();

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

        ConsumerControl info = (ConsumerControl)o;
        info.setDestination((org.apache.activemq.command.ActiveMQDestination) looseUnmarsalNestedObject(wireFormat, dataIn));
        info.setClose(dataIn.readBoolean());
        info.setConsumerId((org.apache.activemq.command.ConsumerId) looseUnmarsalNestedObject(wireFormat, dataIn));
        info.setPrefetch(dataIn.readInt());
        info.setFlush(dataIn.readBoolean());
        info.setStart(dataIn.readBoolean());
        info.setStop(dataIn.readBoolean());

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public void looseMarshal(OpenWireFormat wireFormat, Object o, DataOutput dataOut) throws IOException {

        ConsumerControl info = (ConsumerControl)o;

        super.looseMarshal(wireFormat, o, dataOut);
        looseMarshalNestedObject(wireFormat, (DataStructure)info.getDestination(), dataOut);
        dataOut.writeBoolean(info.isClose());
        looseMarshalNestedObject(wireFormat, (DataStructure)info.getConsumerId(), dataOut);
        dataOut.writeInt(info.getPrefetch());
        dataOut.writeBoolean(info.isFlush());
        dataOut.writeBoolean(info.isStart());
        dataOut.writeBoolean(info.isStop());

    }
}
