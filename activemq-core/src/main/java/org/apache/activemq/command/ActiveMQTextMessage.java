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
package org.apache.activemq.command;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.jms.JMSException;
import javax.jms.MessageNotWriteableException;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * @openwire:marshaller code="28"
 * @version $Revision$
 */
public class ActiveMQTextMessage extends ActiveMQMessage implements TextMessage {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.ACTIVEMQ_TEXT_MESSAGE;

    protected String text;

    public Message copy() {
        ActiveMQTextMessage copy = new ActiveMQTextMessage();
        copy(copy);
        return copy;
    }

    private void copy(ActiveMQTextMessage copy) {
        super.copy(copy);
        copy.text = text;
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public String getJMSXMimeType() {
        return "jms/text-message";
    }

    public void setText(String text) throws MessageNotWriteableException {
        checkReadOnlyBody();
        this.text = text;
        setContent(null);
    }

    public String getText() throws JMSException {
        if (text == null && getContent() != null) {
            InputStream is = null;
            try {
                ByteSequence bodyAsBytes = getContent();
                if (bodyAsBytes != null) {
                    is = new ByteArrayInputStream(bodyAsBytes);
                    if (isCompressed()) {
                        is = new InflaterInputStream(is);
                    }
                    DataInputStream dataIn = new DataInputStream(is);
                    text = MarshallingSupport.readUTF8(dataIn);
                    dataIn.close();
                    setContent(null);
                }
            } catch (IOException ioe) {
                throw JMSExceptionSupport.create(ioe);
            } finally {
                if (is != null) {
                    try {
                        is.close();
                    } catch (IOException e) {
                        // ignore
                    }
                }
            }
        }
        return text;
    }

    public void beforeMarshall(WireFormat wireFormat) throws IOException {
        super.beforeMarshall(wireFormat);

        ByteSequence content = getContent();
        if (content == null && text != null) {
            ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
            OutputStream os = bytesOut;
            ActiveMQConnection connection = getConnection();
            if (connection != null && connection.isUseCompression()) {
                compressed = true;
                os = new DeflaterOutputStream(os);
            }
            DataOutputStream dataOut = new DataOutputStream(os);
            MarshallingSupport.writeUTF8(dataOut, text);
            dataOut.close();
            setContent(bytesOut.toByteSequence());
        }
    }

    /**
     * Clears out the message body. Clearing a message's body does not clear its
     * header values or property entries. <p/>
     * <P>
     * If this message body was read-only, calling this method leaves the
     * message body in the same state as an empty body in a newly created
     * message.
     * 
     * @throws JMSException if the JMS provider fails to clear the message body
     *                 due to some internal error.
     */
    public void clearBody() throws JMSException {
        super.clearBody();
        this.text = null;
    }

    public int getSize() {
        if (size == 0 && content == null && text != null) {
            size = getMinimumMessageSize();
            if (marshalledProperties != null) {
                size += marshalledProperties.getLength();
            }
            size = text.length() * 2;
        }
        return super.getSize();
    }
    
    public String toString() {
        try {
            String text = getText();
        	if (text != null && text.length() > 63) {
        		text = text.substring(0, 45) + "..." + text.substring(text.length() - 12);
        		HashMap<String, Object> overrideFields = new HashMap<String, Object>();
        		overrideFields.put("text", text);
        		return super.toString(overrideFields);
        	}
        } catch (JMSException e) {
        }
        return super.toString();
    }
}
