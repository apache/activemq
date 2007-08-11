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
package org.apache.activemq.transport.xmpp;

import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.wireformat.WireFormat;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A wire format which uses XMPP format of messages
 *
 * @version $Revision$
 */
public class XmppWireFormat implements WireFormat {
    private static final Log log = LogFactory.getLog(XmppWireFormat.class);

    private int version = 1;

    public WireFormat copy() {
        return new XmppWireFormat();
    }

    /*
    public Packet readPacket(DataInput in) throws IOException {
        return null;
    }

    public Packet readPacket(int firstByte, DataInput in) throws IOException {
        return null;
    }

    public Packet writePacket(Packet packet, DataOutput out) throws IOException, JMSException {
        switch (packet.getPacketType()) {
            case Packet.ACTIVEMQ_MESSAGE:
                writeMessage((ActiveMQMessage) packet, "", out);
                break;

            case Packet.ACTIVEMQ_TEXT_MESSAGE:
                writeTextMessage((ActiveMQTextMessage) packet, out);
                break;

            case Packet.ACTIVEMQ_BYTES_MESSAGE:
                writeBytesMessage((ActiveMQBytesMessage) packet, out);
                break;

            case Packet.ACTIVEMQ_OBJECT_MESSAGE:
                writeObjectMessage((ActiveMQObjectMessage) packet, out);
                break;

            case Packet.ACTIVEMQ_MAP_MESSAGE:
            case Packet.ACTIVEMQ_STREAM_MESSAGE:


            case Packet.ACTIVEMQ_BROKER_INFO:
            case Packet.ACTIVEMQ_CONNECTION_INFO:
            case Packet.ACTIVEMQ_MSG_ACK:
            case Packet.CONSUMER_INFO:
            case Packet.DURABLE_UNSUBSCRIBE:
            case Packet.INT_RESPONSE_RECEIPT_INFO:
            case Packet.PRODUCER_INFO:
            case Packet.RECEIPT_INFO:
            case Packet.RESPONSE_RECEIPT_INFO:
            case Packet.SESSION_INFO:
            case Packet.TRANSACTION_INFO:
            case Packet.XA_TRANSACTION_INFO:
            default:
                log.warn("Ignoring message type: " + packet.getPacketType() + " packet: " + packet);
        }
        return null;
    }
*/

//    /**
//     * Can this wireformat process packets of this version
//     * @param version the version number to test
//     * @return true if can accept the version
//     */
//    public boolean canProcessWireFormatVersion(int version){
//        return true;
//    }
//
//    /**
//     * @return the current version of this wire format
//     */
//    public int getCurrentWireFormatVersion(){
//        return 1;
//    }
//
//    // Implementation methods
//    //-------------------------------------------------------------------------
//    protected void writeObjectMessage(ActiveMQObjectMessage message, DataOutput out) throws JMSException, IOException {
//        Serializable object = message.getObject();
//        String text = (object != null) ? object.toString() : "";
//        writeMessage(message, text, out);
//    }
//
//    protected void writeTextMessage(ActiveMQTextMessage message, DataOutput out) throws JMSException, IOException {
//        writeMessage(message, message.getText(), out);
//    }
//
//    protected void writeBytesMessage(ActiveMQBytesMessage message, DataOutput out) throws IOException {
//        ByteArray data = message.getBodyAsBytes();
//        String text = encodeBinary(data.getBuf(),data.getOffset(),data.getLength());
//        writeMessage(message, text, out);
//    }
//
//    protected void writeMessage(ActiveMQMessage message, String body, DataOutput out) throws IOException {
//        String type = getXmppType(message);
//
//        StringBuffer buffer = new StringBuffer("<");
//        buffer.append(type);
//        buffer.append(" to='");
//        buffer.append(message.getJMSDestination().toString());
//        buffer.append("' from='");
//        buffer.append(message.getJMSReplyTo().toString());
//        String messageID = message.getJMSMessageID();
//        if (messageID != null) {
//            buffer.append("' id='");
//            buffer.append(messageID);
//        }
//
//        HashMap properties = message.getProperties();
//        if (properties != null) {
//            for (Iterator iter = properties.entrySet().iterator(); iter.hasNext();) {
//                Map.Entry entry = (Map.Entry) iter.next();
//                Object key = entry.getKey();
//                Object value = entry.getValue();
//                if (value != null) {
//                    buffer.append("' ");
//                    buffer.append(key.toString());
//                    buffer.append("='");
//                    buffer.append(value.toString());
//                }
//            }
//        }
//
//        buffer.append("'>");
//
//        String id = message.getJMSCorrelationID();
//        if (id != null) {
//            buffer.append("<thread>");
//            buffer.append(id);
//            buffer.append("</thread>");
//        }
//        buffer.append(body);
//        buffer.append("</");
//        buffer.append(type);
//        buffer.append(">");
//
//        out.write(buffer.toString().getBytes());
//    }
//
//    protected String encodeBinary(byte[] data,int offset,int length) {
//        // TODO
//        throw new RuntimeException("Not implemented yet!");
//    }
//
//    protected String getXmppType(ActiveMQMessage message) {
//        String type = message.getJMSType();
//        if (type == null) {
//            type = "message";
//        }
//        return type;
//    }


    public ByteSequence marshal(Object command) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);
        marshal(command, dos);
        dos.close();
        return baos.toByteSequence();
    }

    public Object unmarshal(ByteSequence packet) throws IOException {
        ByteArrayInputStream stream = new ByteArrayInputStream(packet);
        DataInputStream dis = new DataInputStream(stream);
        return unmarshal(dis);
    }

    public void marshal(Object object, DataOutput dataOutput) throws IOException {
        /** TODO */
    }

    public Object unmarshal(DataInput dataInput) throws IOException {
        return null;  /** TODO */
    }


    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }
}
