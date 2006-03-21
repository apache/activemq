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

package org.apache.activemq.store.kahadaptor;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.activeio.Packet;
import org.activeio.command.WireFormat;
import org.activeio.packet.ByteArrayPacket;
import org.apache.activemq.kaha.Marshaller;

/**
 * Marshall a Message or a MessageReference
 * @version $Revision: 1.10 $
 */
public class CommandMarshaller implements Marshaller{
    
    private WireFormat wireFormat;
    public CommandMarshaller(WireFormat wireFormat){
        this.wireFormat = wireFormat;
      
    }
    
    public void writePayload(Object object,DataOutputStream dataOut) throws IOException{
        Packet packet = wireFormat.marshal(object);
        byte[] data = packet.sliceAsBytes();
        dataOut.writeInt(data.length);
        dataOut.write(data);
    }

   
    public Object readPayload(DataInputStream dataIn) throws IOException{
        int size=dataIn.readInt();
        byte[] data=new byte[size];
        dataIn.readFully(data);
        return wireFormat.unmarshal(new ByteArrayPacket(data));
    }
}
