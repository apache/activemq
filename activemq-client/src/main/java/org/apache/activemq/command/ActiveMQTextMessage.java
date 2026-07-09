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
 *
 */
public class ActiveMQTextMessage extends ActiveMQMessage implements TextMessage {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.ACTIVEMQ_TEXT_MESSAGE;

    // This is package scope (instead of private) for testing purposes
    volatile String text;

    @Override
    public Message copy() {
        ActiveMQTextMessage copy = new ActiveMQTextMessage();
        synchronized (this) {
            super.copy(copy);
            copy.text = text;
        }
        return copy;
    }

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    @Override
    public String getJMSXMimeType() {
        return "jms/text-message";
    }

    @Override
    public void setText(String text) throws MessageNotWriteableException {
        checkReadOnlyBody();
        synchronized (this) {
            this.text = text;
            setContent(null);
        }
    }

    // Synchronize this to prevent setting content if another mutation operation
    // is happening concurrently.
    @Override
    public synchronized void setContent(ByteSequence content) {
        super.setContent(content);
    }

    @Override
    public String getText() throws JMSException {
        String text = this.text;

        if (text == null) {
            synchronized (this) {
                text = this.text;
                // Double-checked locking, re-check under lock if we need to decode
                if (text == null && content != null) {
                    this.text = text = decodeContent(content);
                    setContent(null);
                    setCompressed(false);
                }
            }
        }

        return text;
    }

    private String decodeContent(ByteSequence bodyAsBytes) throws JMSException {
        String text = null;
        if (bodyAsBytes != null) {
            InputStream is = null;
            try {
                is = new ByteArrayInputStream(bodyAsBytes);
                if (isCompressed()) {
                    // wrap the stream so we don't inflate past maxInflatedDataSize
                    is = MarshallingSupport.createInflaterInputStream(getMaxInflatedDataSize(), is);
                }
                DataInputStream dataIn = new DataInputStream(is);
                text = MarshallingSupport.readUTF8(dataIn);
                dataIn.close();
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

    @Override
    public void beforeMarshall(WireFormat wireFormat) throws IOException {
        super.beforeMarshall(wireFormat);
        storeContentAndClear();
    }

    @Override
    public void storeContentAndClear() {
        // always lock to simplify things because if this method is being called
        // it's right before send so it's very likely to need to mutate state
        // This should generally be uncontested lock so it will be fast
        synchronized (this) {
            storeContent();
            text = null;
        }
    }

    @Override
    public void storeContent() {
        try {
            // Content is volatile so if it's not null we can skip and do nothing
            if (content == null) {
                synchronized (this) {
                    // Double-checked locking, re-check state under lock
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
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // see https://issues.apache.org/activemq/browse/AMQ-2103
    // and https://issues.apache.org/activemq/browse/AMQ-2966
    @Override
    public void clearUnMarshalledState() throws JMSException {
        super.clearUnMarshalledState();
        if (this.text != null) {
            synchronized (this) {
                // This is volatile but another locking makes sure another thread
                // isn't attempting to read this value to marshal to the content at the
                // same time
                this.text = null;
            }
        }
    }

    // We need to sync because both variables need to be read independently
    @Override
    public synchronized boolean isContentMarshalled() {
        return content != null || text == null;
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
    @Override
    public void clearBody() throws JMSException {
        synchronized (this) {
            super.clearBody();
            this.text = null;
        }
    }

    @Override
    public int getSize() {
        int size = this.size;
        if (size == 0) {
            synchronized (this) {
                size = this.size;
                String text = this.text;
                ByteSequence content = getContent();
                if (size == 0 && content == null && text != null) {
                    size = getMinimumMessageSize();
                    ByteSequence marshalledProperties = this.marshalledProperties;
                    if (marshalledProperties != null) {
                        size += marshalledProperties.getLength();
                    }
                    size += text.length() * 2;
                    this.size = size;
                }
                return super.getSize();
            }
        }
        return super.getSize();
    }

    @Override
    public String toString() {
        try {
            String text = this.text;
            if (text == null) {
                text = decodeContent(getContent());
            }
            if (text != null) {
                text = MarshallingSupport.truncate64(text);
                HashMap<String, Object> overrideFields = new HashMap<String, Object>();
                overrideFields.put("text", text);
                return super.toString(overrideFields);
            }
        } catch (JMSException e) {
        }
        return super.toString();
    }

    @SuppressWarnings("unchecked")
    public boolean isBodyAssignableTo(Class c) throws JMSException {
        /*
         * If null the JMS spec says this method always returns true
         * regardless of the passed in class type.
         */
        if (getText() == null) {
            return true;
        }
        return c.isAssignableFrom(java.lang.String.class);
    }

    @SuppressWarnings("unchecked")
    protected <T> T doGetBody(Class<T> asType) throws JMSException {
        return (T) getText();
    }
}
