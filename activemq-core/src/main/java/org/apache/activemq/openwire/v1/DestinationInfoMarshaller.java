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
package org.apache.activemq.openwire.v1;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.activemq.command.*;
import org.apache.activemq.openwire.*;


/**
 * Marshalling code for Open Wire Format for DestinationInfo
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
    public void unmarshal(OpenWireFormat wireFormat, Object o, DataInputStream dataIn, BooleanStream bs) throws IOException {
        super.unmarshal(wireFormat, o, dataIn, bs);

        DestinationInfo info = (DestinationInfo)o;
        info.setConnectionId((org.apache.activemq.command.ConnectionId) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setDestination((org.apache.activemq.command.ActiveMQDestination) unmarsalCachedObject(wireFormat, dataIn, bs));
        info.setOperationType(dataIn.readByte());
        info.setTimeout(unmarshalLong(wireFormat, dataIn, bs));
        
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
        			

    }


    /**
     * Write the booleans that this object uses to a BooleanStream
     */
    public int marshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) throws IOException {

        DestinationInfo info = (DestinationInfo)o;

        int rc = super.marshal1(wireFormat, o, bs);
        rc += marshal1CachedObject(wireFormat, info.getConnectionId(), bs);
        rc += marshal1CachedObject(wireFormat, info.getDestination(), bs);
        
        rc+=marshal1Long(wireFormat, info.getTimeout(), bs);
        rc += marshalObjectArray(wireFormat, info.getBrokerPath(), bs);

        return rc+1;
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

        DestinationInfo info = (DestinationInfo)o;
        marshal2CachedObject(wireFormat, info.getConnectionId(), dataOut, bs);
        marshal2CachedObject(wireFormat, info.getDestination(), dataOut, bs);
        dataOut.writeByte(info.getOperationType());
        marshal2Long(wireFormat, info.getTimeout(), dataOut, bs);
        marshalObjectArray(wireFormat, info.getBrokerPath(), dataOut, bs);

    }
}
