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
package org.apache.activeio.command;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;

import org.apache.activeio.adapter.PacketToInputStream;
import org.apache.activeio.packet.ByteArrayPacket;
import org.apache.activeio.packet.Packet;
import org.apache.activeio.util.ByteArrayOutputStream;

/**
 * A default implementation which uses serialization
 *
 * @version $Revision: 1.1 $
 */
public class DefaultWireFormat implements WireFormat {

    public Packet marshal(Object command) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();        
        DataOutputStream ds = new DataOutputStream(baos);
        marshal(command, ds);
        ds.close();
        return new ByteArrayPacket(baos.toByteSequence());
    }

    public Object unmarshal(Packet packet) throws IOException {
        return unmarshal(new DataInputStream(new PacketToInputStream(packet)));
    }

    public void marshal(Object command, DataOutputStream ds) throws IOException {
        ObjectOutputStream out = new ObjectOutputStream(ds);
        out.writeObject(command);
        out.flush();
        out.reset();
    }

    public Object unmarshal(DataInputStream ds) throws IOException {
        try {
            ClassLoadingAwareObjectInputStream in = new ClassLoadingAwareObjectInputStream(ds);
            Object command;
            command = in.readObject();
            in.close();
            return command;
        } catch (ClassNotFoundException e) {
            throw (IOException)new IOException("unmarshal failed: "+e).initCause(e);
        }
    }

    public void setVersion(int version) {
    }

    public int getVersion() {
        return 0;
    }

}
