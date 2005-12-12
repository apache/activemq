/** 
 * 
 * Copyright 2004 Protique Ltd
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
package org.activemq.transport.xstream;

import com.thoughtworks.xstream.XStream;

import org.activeio.Packet;
import org.activeio.command.WireFormat;
import org.activemq.command.Command;
import org.activemq.transport.util.TextWireFormat;

import javax.jms.JMSException;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Reader;

/**
 * A {@link WireFormat} implementation which uses the <a
 * href="http://xstream.codehaus.org/>XStream</a> library to marshall commands
 * onto the wire
 * 
 * @version $Revision$
 */
public class XStreamWireFormat extends TextWireFormat {
    private XStream xStream;
    private int version;

    public void marshal(Object command, DataOutputStream out) throws IOException {
        String text = getXStream().toXML(command);
        out.writeUTF(text);
    }

    public Packet marshal(Object command) throws IOException {
        return null;
    }

    public Object unmarshal(DataInputStream arg0) throws IOException {
        return null;
    }

    public Object unmarshal(Packet arg0) throws IOException {
        return null;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public Packet readPacket(DataInput in) throws IOException {
        String text = in.readUTF();
        return (Packet) getXStream().fromXML(text);
    }

    public Packet readPacket(int firstByte, DataInput in) throws IOException {
        String text = in.readUTF();
        return (Packet) getXStream().fromXML(text);
    }

    public Packet writePacket(Packet packet, DataOutput out) throws IOException, JMSException {
        String text = getXStream().toXML(packet);
        out.writeUTF(text);
        return null;
    }

    public WireFormat copy() {
        return new XStreamWireFormat();
    }

    public String toString(Packet packet) {
        return getXStream().toXML(packet);
    }

    public Packet fromString(String xml) {
        return (Packet) getXStream().fromXML(xml);
    }

    public Command readCommand(String text) {
        return (Command) getXStream().fromXML(text);
    }
    
    public Command readCommand(Reader reader) {
        return (Command) getXStream().fromXML(reader);
    }

    public String toString(Command command) {
        return getXStream().toXML(command);
    }

    /**
     * Can this wireformat process packets of this version
     * 
     * @param version
     *            the version number to test
     * @return true if can accept the version
     */
    public boolean canProcessWireFormatVersion(int version) {
        return true;
    }

    /**
     * @return the current version of this wire format
     */
    public int getCurrentWireFormatVersion() {
        return 1;
    }

    // Properties
    // -------------------------------------------------------------------------
    public XStream getXStream() {
        if (xStream == null) {
            xStream = createXStream();
        }
        return xStream;
    }

    public void setXStream(XStream xStream) {
        this.xStream = xStream;
    }

    // Implementation methods
    // -------------------------------------------------------------------------
    protected XStream createXStream() {
        return new XStream();
    }

}
