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
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.jms.JMSException;
import javax.jms.ObjectMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ClassLoadingAwareObjectInputStream;
import org.apache.activemq.util.JMSExceptionSupport;

/**
 * An <CODE>ObjectMessage</CODE> object is used to send a message that
 * contains a serializable object in the Java programming language ("Java
 * object"). It inherits from the <CODE>Message</CODE> interface and adds a
 * body containing a single reference to an object. Only
 * <CODE>Serializable</CODE> Java objects can be used. <p/>
 * <P>
 * If a collection of Java objects must be sent, one of the
 * <CODE>Collection</CODE> classes provided since JDK 1.2 can be used. <p/>
 * <P>
 * When a client receives an <CODE>ObjectMessage</CODE>, it is in read-only
 * mode. If a client attempts to write to the message at this point, a
 * <CODE>MessageNotWriteableException</CODE> is thrown. If
 * <CODE>clearBody</CODE> is called, the message can now be both read from and
 * written to.
 * 
 * @openwire:marshaller code="26"
 * @see javax.jms.Session#createObjectMessage()
 * @see javax.jms.Session#createObjectMessage(Serializable)
 * @see javax.jms.BytesMessage
 * @see javax.jms.MapMessage
 * @see javax.jms.Message
 * @see javax.jms.StreamMessage
 * @see javax.jms.TextMessage
 */
public class ActiveMQObjectMessage extends ActiveMQMessage implements ObjectMessage {
    static final ClassLoader ACTIVEMQ_CLASSLOADER = ActiveMQObjectMessage.class.getClassLoader(); // TODO
                                                                                                    // verify
                                                                                                    // classloader
    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.ACTIVEMQ_OBJECT_MESSAGE;

    protected transient Serializable object;

    public Message copy() {
        ActiveMQObjectMessage copy = new ActiveMQObjectMessage();
        copy(copy);
        return copy;
    }

    private void copy(ActiveMQObjectMessage copy) {
        storeContent();
        super.copy(copy);
        copy.object = null;
    }

    public void storeContent() {
        ByteSequence bodyAsBytes = getContent();
        if (bodyAsBytes == null && object != null) {
            try {
                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                OutputStream os = bytesOut;
                ActiveMQConnection connection = getConnection();
                if (connection != null && connection.isUseCompression()) {
                    compressed = true;
                    os = new DeflaterOutputStream(os);
                }
                DataOutputStream dataOut = new DataOutputStream(os);
                ObjectOutputStream objOut = new ObjectOutputStream(dataOut);
                objOut.writeObject(object);
                objOut.flush();
                objOut.reset();
                objOut.close();
                setContent(bytesOut.toByteSequence());
            } catch (IOException ioe) {
                throw new RuntimeException(ioe.getMessage(), ioe);
            }
        }
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public String getJMSXMimeType() {
        return "jms/object-message";
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
        this.object = null;
    }

    /**
     * Sets the serializable object containing this message's data. It is
     * important to note that an <CODE>ObjectMessage</CODE> contains a
     * snapshot of the object at the time <CODE>setObject()</CODE> is called;
     * subsequent modifications of the object will have no effect on the
     * <CODE>ObjectMessage</CODE> body.
     * 
     * @param newObject the message's data
     * @throws JMSException if the JMS provider fails to set the object due to
     *                 some internal error.
     * @throws javax.jms.MessageFormatException if object serialization fails.
     * @throws javax.jms.MessageNotWriteableException if the message is in
     *                 read-only mode.
     */

    public void setObject(Serializable newObject) throws JMSException {
        checkReadOnlyBody();
        this.object = newObject;
        setContent(null);
        ActiveMQConnection connection = getConnection();
        if (connection == null || !connection.isObjectMessageSerializationDefered()) {
            storeContent();
        }
    }

    /**
     * Gets the serializable object containing this message's data. The default
     * value is null.
     * 
     * @return the serializable object containing this message's data
     * @throws JMSException
     */
    public Serializable getObject() throws JMSException {
        if (object == null && getContent() != null) {
            try {
                ByteSequence content = getContent();
                InputStream is = new ByteArrayInputStream(content);
                if (isCompressed()) {
                    is = new InflaterInputStream(is);
                }
                DataInputStream dataIn = new DataInputStream(is);
                ClassLoadingAwareObjectInputStream objIn = new ClassLoadingAwareObjectInputStream(dataIn);
                try {
                    object = (Serializable)objIn.readObject();
                } catch (ClassNotFoundException ce) {
                    throw new IOException(ce.getMessage());
                }
                dataIn.close();
            } catch (IOException e) {
                throw JMSExceptionSupport.create("Failed to build body from bytes. Reason: " + e, e);
            }
        }
        return this.object;
    }

    public void onMessageRolledBack() {
        super.onMessageRolledBack();

        // lets force the object to be deserialized again - as we could have
        // changed the object
        object = null;
    }

    public String toString() {
        try {
            getObject();
        } catch (JMSException e) {
        }
        return super.toString();
    }
}
