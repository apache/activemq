/**
* <a href="http://activemq.org">ActiveMQ: The Open Source Message Fabric</a>
*
* Copyright 2005 (C) LogicBlaze, Inc. http://www.logicblaze.com
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
package org.activemq.openwire;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

final public class BooleanStream {

    byte data[] = new byte[48];
    short arrayLimit;    
    short arrayPos;    
    byte bytePos;
    
    public boolean readBoolean() throws IOException {
        assert arrayPos <= arrayLimit;
        byte b = data[arrayPos];
        boolean rc = ((b>>bytePos)&0x01)!=0;
        bytePos++;
        if( bytePos >= 8 ) {
            bytePos=0;
            arrayPos++;
        }
        return rc;
    }
    
    public void writeBoolean(boolean value) throws IOException {
        if( bytePos == 0 ) {
            arrayLimit++;
            if( arrayLimit >= data.length ) {
                // re-grow the array.
                byte d[] = new byte[data.length*2];
                System.arraycopy(data, 0, d, 0, data.length);
                data = d;
            }
        }
        if( value ) {
            data[arrayPos] |= (0x01 << bytePos); 
        }
        bytePos++;
        if( bytePos >= 8 ) {
            bytePos=0;
            arrayPos++;
        }
    }
    
    public void marshal(DataOutputStream dataOut) throws IOException {
        if( arrayLimit < 64 ) {
            dataOut.writeByte(arrayLimit);
        } else if( arrayLimit < 256 ) { // max value of unsigned byte
            dataOut.writeByte(0xC0);
            dataOut.writeByte(arrayLimit);            
        } else {
            dataOut.writeByte(0xE0);
            dataOut.writeShort(arrayLimit);            
        }
        
        dataOut.write(data, 0, arrayLimit);
        clear();
    }
    
    public void unmarshal(DataInputStream dataIn) throws IOException {
        
        arrayLimit = dataIn.readByte();
        if( (arrayLimit & 0xE0)!=0 ) {
            arrayLimit = dataIn.readShort();
        } else if ( (arrayLimit & 0xC0)!=0 ) {
            arrayLimit = (short)(dataIn.readByte() & 0xFF);
        } 
        if( data.length < arrayLimit ) {
            data = new byte[arrayLimit];
        }
        dataIn.readFully(data, 0, arrayLimit);
        clear();
    }
    
    public void clear() {
        arrayPos=0;
        bytePos=0;
    }

    public int marshalledSize() {
        if( arrayLimit < 64 ) {
            return 1+arrayLimit;
        } else {
            return 2+arrayLimit;
        }
    }


}
