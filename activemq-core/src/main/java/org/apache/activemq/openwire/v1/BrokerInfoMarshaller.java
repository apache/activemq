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
 * Marshalling code for Open Wire Format for BrokerInfo
 *
 *
 * NOTE!: This file is auto generated - do not modify!
 *        if you need to make a change, please see the modify the groovy scripts in the
 *        under src/gram/script and then use maven openwire:generate to regenerate 
 *        this file.
 *
 * @version $Revision$
 */
public class BrokerInfoMarshaller extends BaseCommandMarshaller {

    /**
     * Return the type of Data Structure we marshal
     * @return short representation of the type data structure
     */
    public byte getDataStructureType() {
        return BrokerInfo.DATA_STRUCTURE_TYPE;
    }
    
    /**
     * @return a new object instance
     */
    public DataStructure createObject() {
        return new BrokerInfo();
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

        BrokerInfo info = (BrokerInfo)o;
        info.setBrokerId((org.apache.activemq.command.BrokerId) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setBrokerURL(readString(dataIn, bs));
        
		        if( bs.readBoolean() ) {
		            short size = dataIn.readShort();
		            org.apache.activemq.command.BrokerInfo value[] = new org.apache.activemq.command.BrokerInfo[size];
		            for( int i=0; i < size; i++ ) {
		                value[i] = (org.apache.activemq.command.BrokerInfo)unmarsalNestedObject(wireFormat,dataIn, bs);
		            }
		            info.setPeerBrokerInfos(value);
		        } else {
		            info.setPeerBrokerInfos(null);
		        }
        			
        info.setBrokerName(readString(dataIn, bs));

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int marshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        BrokerInfo info = (BrokerInfo)o;

        int rc = super.marshal1(wireFormat, o, bs);
        rc += marshal1CachedObject(wireFormat, info.getBrokerId(), bs);
        rc += writeString(info.getBrokerURL(), bs);
        rc += marshalObjectArray(wireFormat, info.getPeerBrokerInfos(), bs);
        rc += writeString(info.getBrokerName(), bs);

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

        BrokerInfo info = (BrokerInfo)o;
        marshal2CachedObject(wireFormat, info.getBrokerId(), dataOut, bs);
        writeString(info.getBrokerURL(), dataOut, bs);
        marshalObjectArray(wireFormat, info.getPeerBrokerInfos(), dataOut, bs);
        writeString(info.getBrokerName(), dataOut, bs);

    }
}
