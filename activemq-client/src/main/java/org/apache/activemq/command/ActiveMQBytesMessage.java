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
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MessageEOFException;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotReadableException;
import javax.jms.MessageNotWriteableException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.ByteSequenceData;
import org.apache.activemq.util.JMSExceptionSupport;

/**
 * A <CODE>BytesMessage</CODE> object is used to send a message containing a
 * stream of uninterpreted bytes. It inherits from the <CODE>Message</CODE>
 * interface and adds a bytes message body. The receiver of the message supplies
 * the interpretation of the bytes.
 * <P>
 * The <CODE>BytesMessage</CODE> methods are based largely on those found in
 * <CODE>java.io.DataInputStream</CODE> and
 * <CODE>java.io.DataOutputStream</CODE>.
 * <P>
 * This message type is for client encoding of existing message formats. If
 * possible, one of the other self-defining message types should be used
 * instead.
 * <P>
 * Although the JMS API allows the use of message properties with byte messages,
 * they are typically not used, since the inclusion of properties may affect the
 * format.
 * <P>
 * The primitive types can be written explicitly using methods for each type.
 * They may also be written generically as objects. For instance, a call to
 * <CODE>BytesMessage.writeInt(6)</CODE> is equivalent to
 * <CODE> BytesMessage.writeObject(new Integer(6))</CODE>. Both forms are
 * provided, because the explicit form is convenient for static programming, and
 * the object form is needed when types are not known at compile time.
 * <P>
 * When the message is first created, and when <CODE>clearBody</CODE> is
 * called, the body of the message is in write-only mode. After the first call
 * to <CODE>reset</CODE> has been made, the message body is in read-only mode.
 * After a message has been sent, the client that sent it can retain and modify
 * it without affecting the message that has been sent. The same message object
 * can be sent multiple times. When a message has been received, the provider
 * has called <CODE>reset</CODE> so that the message body is in read-only mode
 * for the client.
 * <P>
 * If <CODE>clearBody</CODE> is called on a message in read-only mode, the
 * message body is cleared and the message is in write-only mode.
 * <P>
 * If a client attempts to read a message in write-only mode, a
 * <CODE>MessageNotReadableException</CODE> is thrown.
 * <P>
 * If a client attempts to write a message in read-only mode, a
 * <CODE>MessageNotWriteableException</CODE> is thrown.
 *
 * @openwire:marshaller code=24
 * @see javax.jms.Session#createBytesMessage()
 * @see javax.jms.MapMessage
 * @see javax.jms.Message
 * @see javax.jms.ObjectMessage
 * @see javax.jms.StreamMessage
 * @see javax.jms.TextMessage
 */
public class ActiveMQBytesMessage extends ActiveMQMessage implements BytesMessage {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.ACTIVEMQ_BYTES_MESSAGE;

    protected transient DataOutputStream dataOut;
    protected transient ByteArrayOutputStream bytesOut;
    protected transient DataInputStream dataIn;
    protected transient int length;

    @Override
    public Message copy() {
        ActiveMQBytesMessage copy = new ActiveMQBytesMessage();
        copy(copy);
        return copy;
    }

    private void copy(ActiveMQBytesMessage copy) {
        storeContent();
        super.copy(copy);
        copy.dataOut = null;
        copy.bytesOut = null;
        copy.dataIn = null;
    }

    @Override
    public void onSend() throws JMSException {
        super.onSend();
        storeContent();
    }

    @Override
    public void storeContent() {
        if (dataOut != null) {
            try {
                dataOut.close();
                ByteSequence bs = bytesOut.toByteSequence();
                setContent(bs);

                ActiveMQConnection connection = getConnection();
                if (connection != null && connection.isUseCompression()) {
                    doCompress();
                }
            } catch (IOException ioe) {
                throw new RuntimeException(ioe.getMessage(), ioe);
            } finally {
                try {
                    if (bytesOut != null) {
                        bytesOut.close();
                        bytesOut = null;
                    }
                    if (dataOut != null) {
                        dataOut.close();
                        dataOut = null;
                    }
                } catch (IOException ioe) {
                }
            }
        }
    }

    @Override
    public boolean isContentMarshalled() {
        return content != null || dataOut == null;
    }

    @Override
    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    @Override
    public String getJMSXMimeType() {
        return "jms/bytes-message";
    }

    /**
     * Clears out the message body. Clearing a message's body does not clear its
     * header values or property entries.
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
        super.clearBody();
        this.dataOut = null;
        this.dataIn = null;
        this.bytesOut = null;
    }

    /**
     * Gets the number of bytes of the message body when the message is in
     * read-only mode. The value returned can be used to allocate a byte array.
     * The value returned is the entire length of the message body, regardless
     * of where the pointer for reading the message is currently located.
     *
     * @return number of bytes in the message
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageNotReadableException if the message is in write-only mode.
     * @since 1.1
     */

    @Override
    public long getBodyLength() throws JMSException {
        initializeReading();
        return length;
    }

    /**
     * Reads a <code>boolean</code> from the bytes message stream.
     *
     * @return the <code>boolean</code> value read
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public boolean readBoolean() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readBoolean();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads a signed 8-bit value from the bytes message stream.
     *
     * @return the next byte from the bytes message stream as a signed 8-bit
     *         <code>byte</code>
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public byte readByte() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readByte();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads an unsigned 8-bit number from the bytes message stream.
     *
     * @return the next byte from the bytes message stream, interpreted as an
     *         unsigned 8-bit number
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public int readUnsignedByte() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readUnsignedByte();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads a signed 16-bit number from the bytes message stream.
     *
     * @return the next two bytes from the bytes message stream, interpreted as
     *         a signed 16-bit number
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public short readShort() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readShort();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads an unsigned 16-bit number from the bytes message stream.
     *
     * @return the next two bytes from the bytes message stream, interpreted as
     *         an unsigned 16-bit integer
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public int readUnsignedShort() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readUnsignedShort();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads a Unicode character value from the bytes message stream.
     *
     * @return the next two bytes from the bytes message stream as a Unicode
     *         character
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public char readChar() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readChar();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads a signed 32-bit integer from the bytes message stream.
     *
     * @return the next four bytes from the bytes message stream, interpreted as
     *         an <code>int</code>
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public int readInt() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readInt();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads a signed 64-bit integer from the bytes message stream.
     *
     * @return the next eight bytes from the bytes message stream, interpreted
     *         as a <code>long</code>
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public long readLong() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readLong();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads a <code>float</code> from the bytes message stream.
     *
     * @return the next four bytes from the bytes message stream, interpreted as
     *         a <code>float</code>
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public float readFloat() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readFloat();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads a <code>double</code> from the bytes message stream.
     *
     * @return the next eight bytes from the bytes message stream, interpreted
     *         as a <code>double</code>
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public double readDouble() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readDouble();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads a string that has been encoded using a modified UTF-8 format from
     * the bytes message stream.
     * <P>
     * For more information on the UTF-8 format, see "File System Safe UCS
     * Transformation Format (FSS_UTF)", X/Open Preliminary Specification,
     * X/Open Company Ltd., Document Number: P316. This information also appears
     * in ISO/IEC 10646, Annex P.
     *
     * @return a Unicode string from the bytes message stream
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageEOFException if unexpected end of bytes stream has been
     *                 reached.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public String readUTF() throws JMSException {
        initializeReading();
        try {
            return this.dataIn.readUTF();
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Reads a byte array from the bytes message stream.
     * <P>
     * If the length of array <code>value</code> is less than the number of
     * bytes remaining to be read from the stream, the array should be filled. A
     * subsequent call reads the next increment, and so on.
     * <P>
     * If the number of bytes remaining in the stream is less than the length of
     * array <code>value</code>, the bytes should be read into the array. The
     * return value of the total number of bytes read will be less than the
     * length of the array, indicating that there are no more bytes left to be
     * read from the stream. The next read of the stream returns -1.
     *
     * @param value the buffer into which the data is read
     * @return the total number of bytes read into the buffer, or -1 if there is
     *         no more data because the end of the stream has been reached
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public int readBytes(byte[] value) throws JMSException {
        return readBytes(value, value.length);
    }

    /**
     * Reads a portion of the bytes message stream.
     * <P>
     * If the length of array <code>value</code> is less than the number of
     * bytes remaining to be read from the stream, the array should be filled. A
     * subsequent call reads the next increment, and so on.
     * <P>
     * If the number of bytes remaining in the stream is less than the length of
     * array <code>value</code>, the bytes should be read into the array. The
     * return value of the total number of bytes read will be less than the
     * length of the array, indicating that there are no more bytes left to be
     * read from the stream. The next read of the stream returns -1. <p/> If
     * <code>length</code> is negative, or <code>length</code> is greater
     * than the length of the array <code>value</code>, then an
     * <code>IndexOutOfBoundsException</code> is thrown. No bytes will be read
     * from the stream for this exception case.
     *
     * @param value the buffer into which the data is read
     * @param length the number of bytes to read; must be less than or equal to
     *                <code>value.length</code>
     * @return the total number of bytes read into the buffer, or -1 if there is
     *         no more data because the end of the stream has been reached
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageNotReadableException if the message is in write-only mode.
     */
    @Override
    public int readBytes(byte[] value, int length) throws JMSException {
        initializeReading();
        try {
            int n = 0;
            while (n < length) {
                int count = this.dataIn.read(value, n, length - n);
                if (count < 0) {
                    break;
                }
                n += count;
            }
            if (n == 0 && length > 0) {
                n = -1;
            }
            return n;
        } catch (EOFException e) {
            throw JMSExceptionSupport.createMessageEOFException(e);
        } catch (IOException e) {
            throw JMSExceptionSupport.createMessageFormatException(e);
        }
    }

    /**
     * Writes a <code>boolean</code> to the bytes message stream as a 1-byte
     * value. The value <code>true</code> is written as the value
     * <code>(byte)1</code>; the value <code>false</code> is written as the
     * value <code>(byte)0</code>.
     *
     * @param value the <code>boolean</code> value to be written
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeBoolean(boolean value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeBoolean(value);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Writes a <code>byte</code> to the bytes message stream as a 1-byte
     * value.
     *
     * @param value the <code>byte</code> value to be written
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeByte(byte value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeByte(value);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Writes a <code>short</code> to the bytes message stream as two bytes,
     * high byte first.
     *
     * @param value the <code>short</code> to be written
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeShort(short value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeShort(value);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Writes a <code>char</code> to the bytes message stream as a 2-byte
     * value, high byte first.
     *
     * @param value the <code>char</code> value to be written
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeChar(char value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeChar(value);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Writes an <code>int</code> to the bytes message stream as four bytes,
     * high byte first.
     *
     * @param value the <code>int</code> to be written
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeInt(int value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeInt(value);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Writes a <code>long</code> to the bytes message stream as eight bytes,
     * high byte first.
     *
     * @param value the <code>long</code> to be written
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeLong(long value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeLong(value);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Converts the <code>float</code> argument to an <code>int</code> using
     * the <code>floatToIntBits</code> method in class <code>Float</code>,
     * and then writes that <code>int</code> value to the bytes message stream
     * as a 4-byte quantity, high byte first.
     *
     * @param value the <code>float</code> value to be written
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeFloat(float value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeFloat(value);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Converts the <code>double</code> argument to a <code>long</code>
     * using the <code>doubleToLongBits</code> method in class
     * <code>Double</code>, and then writes that <code>long</code> value to
     * the bytes message stream as an 8-byte quantity, high byte first.
     *
     * @param value the <code>double</code> value to be written
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeDouble(double value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeDouble(value);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Writes a string to the bytes message stream using UTF-8 encoding in a
     * machine-independent manner.
     * <P>
     * For more information on the UTF-8 format, see "File System Safe UCS
     * Transformation Format (FSS_UTF)", X/Open Preliminary Specification,
     * X/Open Company Ltd., Document Number: P316. This information also appears
     * in ISO/IEC 10646, Annex P.
     *
     * @param value the <code>String</code> value to be written
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeUTF(String value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.writeUTF(value);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Writes a byte array to the bytes message stream.
     *
     * @param value the byte array to be written
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeBytes(byte[] value) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(value);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Writes a portion of a byte array to the bytes message stream.
     *
     * @param value the byte array value to be written
     * @param offset the initial offset within the byte array
     * @param length the number of bytes to use
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    @Override
    public void writeBytes(byte[] value, int offset, int length) throws JMSException {
        initializeWriting();
        try {
            this.dataOut.write(value, offset, length);
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    /**
     * Writes an object to the bytes message stream.
     * <P>
     * This method works only for the objectified primitive object types (<code>Integer</code>,<code>Double</code>,
     * <code>Long</code> &nbsp;...), <code>String</code> objects, and byte
     * arrays.
     *
     * @param value the object in the Java programming language ("Java object")
     *                to be written; it must not be null
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws MessageFormatException if the object is of an invalid type.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     * @throws java.lang.NullPointerException if the parameter
     *                 <code>value</code> is null.
     */
    @Override
    public void writeObject(Object value) throws JMSException {
        if (value == null) {
            throw new NullPointerException();
        }
        initializeWriting();
        if (value instanceof Boolean) {
            writeBoolean(((Boolean)value).booleanValue());
        } else if (value instanceof Character) {
            writeChar(((Character)value).charValue());
        } else if (value instanceof Byte) {
            writeByte(((Byte)value).byteValue());
        } else if (value instanceof Short) {
            writeShort(((Short)value).shortValue());
        } else if (value instanceof Integer) {
            writeInt(((Integer)value).intValue());
        } else if (value instanceof Long) {
            writeLong(((Long)value).longValue());
        } else if (value instanceof Float) {
            writeFloat(((Float)value).floatValue());
        } else if (value instanceof Double) {
            writeDouble(((Double)value).doubleValue());
        } else if (value instanceof String) {
            writeUTF(value.toString());
        } else if (value instanceof byte[]) {
            writeBytes((byte[])value);
        } else {
            throw new MessageFormatException("Cannot write non-primitive type:" + value.getClass());
        }
    }

    /**
     * Puts the message body in read-only mode and repositions the stream of
     * bytes to the beginning.
     *
     * @throws JMSException if an internal error occurs
     */
    @Override
    public void reset() throws JMSException {
        storeContent();
        setReadOnlyBody(true);
        try {
            if (bytesOut != null) {
                bytesOut.close();
                bytesOut = null;
            }
            if (dataIn != null) {
                dataIn.close();
                dataIn = null;
            }
            if (dataOut != null) {
                dataOut.close();
                dataOut = null;
            }
        } catch (IOException ioe) {
            throw JMSExceptionSupport.create(ioe);
        }
    }

    private void initializeWriting() throws JMSException {
        checkReadOnlyBody();
        initializeWritingNoCheck();
    }

    private void initializeWritingNoCheck() throws JMSException {
        if (this.dataOut == null) {
            this.bytesOut = new ByteArrayOutputStream();
            OutputStream os = bytesOut;
            this.dataOut = new DataOutputStream(os);
        }

        restoreOldContent();
    }

    private void restoreOldContent() throws JMSException {
        // For a message that already had a body and was sent we need to restore the content
        // if the message is used again without having its clearBody method called.
        if (this.content != null && this.content.length > 0) {
            try {
                ByteSequence toRestore = this.content;
                if (isCompressed()) {
                    toRestore = new ByteSequence(decompress(this.content));
                    compressed = false;
                }

                this.dataOut.write(toRestore.getData(), toRestore.getOffset(), toRestore.getLength());
                // Free up the buffer from the old content, will be re-written when
                // the message is sent again and storeContent() is called.
                this.content = null;
            } catch (IOException ioe) {
                throw JMSExceptionSupport.create(ioe);
            }
        }
    }

    protected void checkWriteOnlyBody() throws MessageNotReadableException {
        if (!readOnlyBody) {
            throw new MessageNotReadableException("Message body is write-only");
        }
    }

    private void initializeReading() throws JMSException {
        checkWriteOnlyBody();
        if (dataIn == null) {
            try {
                ByteSequence data = getContent();
                if (data == null) {
                    data = new ByteSequence(new byte[] {}, 0, 0);
                }
                InputStream is = new ByteArrayInputStream(data);
                if (isCompressed()) {
                    if (data.length != 0) {
                        is = new ByteArrayInputStream(decompress(data));
                    }
                } else {
                    length = data.getLength();
                }

                dataIn = new DataInputStream(is);
            } catch (IOException ioe) {
                throw JMSExceptionSupport.create(ioe);
            }
        }
    }

    protected byte[] decompress(ByteSequence dataSequence) throws IOException {
        Inflater inflater = new Inflater();
        ByteArrayOutputStream decompressed = new ByteArrayOutputStream();
        try {
            length = ByteSequenceData.readIntBig(dataSequence);
            dataSequence.offset = 0;
            byte[] data = Arrays.copyOfRange(dataSequence.getData(), 4, dataSequence.getLength());
            inflater.setInput(data);
            byte[] buffer = new byte[length];
            int count = inflater.inflate(buffer);
            decompressed.write(buffer, 0, count);
            return decompressed.toByteArray();
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            inflater.end();
            decompressed.close();
        }
    }

    @Override
    public void setObjectProperty(String name, Object value) throws JMSException {
        checkReadOnlyProperties();
        initializeWritingNoCheck();
        super.setObjectProperty(name, value);
    }

    @Override
    public String toString() {
        return super.toString() + " ActiveMQBytesMessage{ " + "bytesOut = " + bytesOut + ", dataOut = " + dataOut + ", dataIn = " + dataIn + " }";
    }

    @Override
    protected void doCompress() throws IOException {
        compressed = true;
        ByteSequence bytes = getContent();
        if (bytes != null) {
            int length = bytes.getLength();
            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
            compressed.write(new byte[4]);
            Deflater deflater = new Deflater();
            try {
                deflater.setInput(bytes.data);
                deflater.finish();
                byte[] buffer = new byte[1024];
                while (!deflater.finished()) {
                    int count = deflater.deflate(buffer);
                    compressed.write(buffer, 0, count);
                }

                bytes = compressed.toByteSequence();
                ByteSequenceData.writeIntBig(bytes, length);
                bytes.offset = 0;
                setContent(bytes);
            } finally {
                deflater.end();
                compressed.close();
            }
        }
    }
}
