/** 
 * <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a> 
 * 
 * Copyright 2005 Hiram Chirino
 * Copyright 2005 Protique Ltd
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
 * 
 **/
package org.apache.activemq.openwire.v1;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.openwire.*;
import org.apache.activemq.command.*;


/**
 * Marshalling code for Open Wire Format for ConsumerInfo
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version $Revision$
 */
public class ConsumerInfoMarshaller extends BaseCommandMarshaller {

    /**
     * Return the type of Data Structure we marshal
     * @return short representation of the type data structure
     */
    public byte getDataStructureType() {
        return ConsumerInfo.DATA_STRUCTURE_TYPE;
    }
    
    /**
     * @return a new object instance
     */
    public DataStructure createObject() {
        return new ConsumerInfo();
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

        ConsumerInfo info = (ConsumerInfo)o;
        info.setConsumerId((org.apache.activemq.command.ConsumerId) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setBrowser(bs.readBoolean());
        info.setDestination((org.apache.activemq.command.ActiveMQDestination) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setPrefetchSize(dataIn.readInt());
        info.setDispatchAsync(bs.readBoolean());
        info.setSelector(readString(dataIn, bs));
        info.setSubcriptionName(readString(dataIn, bs));
        info.setNoLocal(bs.readBoolean());
        info.setExclusive(bs.readBoolean());
        info.setRetroactive(bs.readBoolean());
        info.setPriority(dataIn.readByte());
        
		        if( bs.readBoolean() ) {
		            short size = dataIn.readShort();
		            org.apache.activemq.command.BrokerId value[] = new org.apache.activemq.command.BrokerId[size];
		            for( int i=0; i < size; i++ ) {
		                value[i] = (org.apache.activemq.command.BrokerId)unmarsalNestedObject(wireFormat,dataIn, bs);
		            }
		            info.setBrokerPath(value);
		        } else {
		            info.setBrokerPath(null);
		        }
        			
        info.setNetworkSubscription(bs.readBoolean());

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int marshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        ConsumerInfo info = (ConsumerInfo)o;

        int rc = super.marshal1(wireFormat, o, bs);
        rc += marshal1CachedObject(wireFormat, info.getConsumerId(), bs);
        bs.writeBoolean(info.isBrowser());
        rc += marshal1CachedObject(wireFormat, info.getDestination(), bs);
        
        bs.writeBoolean(info.isDispatchAsync());
        rc += writeString(info.getSelector(), bs);
        rc += writeString(info.getSubcriptionName(), bs);
        bs.writeBoolean(info.isNoLocal());
        bs.writeBoolean(info.isExclusive());
        bs.writeBoolean(info.isRetroactive());
        
        rc += marshalObjectArray(wireFormat, info.getBrokerPath(), bs);
        bs.writeBoolean(info.isNetworkSubscription());

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

        ConsumerInfo info = (ConsumerInfo)o;
        marshal2CachedObject(wireFormat, info.getConsumerId(), dataOut, bs);
        bs.readBoolean();
        marshal2CachedObject(wireFormat, info.getDestination(), dataOut, bs);
        dataOut.writeInt(info.getPrefetchSize());
        bs.readBoolean();
        writeString(info.getSelector(), dataOut, bs);
        writeString(info.getSubcriptionName(), dataOut, bs);
        bs.readBoolean();
        bs.readBoolean();
        bs.readBoolean();
        dataOut.writeByte(info.getPriority());
        marshalObjectArray(wireFormat, info.getBrokerPath(), dataOut, bs);
        bs.readBoolean();

    }
}
