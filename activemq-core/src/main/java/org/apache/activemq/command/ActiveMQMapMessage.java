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
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.InflaterInputStream;

import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageFormatException;
import javax.jms.MessageNotWriteableException;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.util.ByteArrayInputStream;
import org.apache.activemq.util.ByteArrayOutputStream;
import org.apache.activemq.util.ByteSequence;
import org.apache.activemq.util.JMSExceptionSupport;
import org.apache.activemq.util.MarshallingSupport;
import org.apache.activemq.wireformat.WireFormat;

/**
 * A <CODE>MapMessage</CODE> object is used to send a set of name-value pairs.
 * The names are <CODE>String</CODE> objects, and the values are primitive
 * data types in the Java programming language. The names must have a value that
 * is not null, and not an empty string. The entries can be accessed
 * sequentially or randomly by name. The order of the entries is undefined.
 * <CODE>MapMessage</CODE> inherits from the <CODE>Message</CODE> interface
 * and adds a message body that contains a Map.
 * <P>
 * The primitive types can be read or written explicitly using methods for each
 * type. They may also be read or written generically as objects. For instance,
 * a call to <CODE>MapMessage.setInt("foo", 6)</CODE> is equivalent to
 * <CODE> MapMessage.setObject("foo", new Integer(6))</CODE>. Both forms are
 * provided, because the explicit form is convenient for static programming, and
 * the object form is needed when types are not known at compile time.
 * <P>
 * When a client receives a <CODE>MapMessage</CODE>, it is in read-only mode.
 * If a client attempts to write to the message at this point, a
 * <CODE>MessageNotWriteableException</CODE> is thrown. If
 * <CODE>clearBody</CODE> is called, the message can now be both read from and
 * written to.
 * <P>
 * <CODE>MapMessage</CODE> objects support the following conversion table. The
 * marked cases must be supported. The unmarked cases must throw a
 * <CODE>JMSException</CODE>. The <CODE>String</CODE> -to-primitive
 * conversions may throw a runtime exception if the primitive's
 * <CODE>valueOf()</CODE> method does not accept it as a valid
 * <CODE> String</CODE> representation of the primitive.
 * <P>
 * A value written as the row type can be read as the column type. <p/>
 * 
 * <PRE>
 * | | boolean byte short char int long float double String byte[] |----------------------------------------------------------------------
 * |boolean | X X |byte | X X X X X |short | X X X X |char | X X |int | X X X |long | X X |float | X X X |double | X X
 * |String | X X X X X X X X |byte[] | X |----------------------------------------------------------------------
 * &lt;p/&gt;
 * </PRE>
 * 
 * <p/>
 * <P>
 * Attempting to read a null value as a primitive type must be treated as
 * calling the primitive's corresponding <code>valueOf(String)</code>
 * conversion method with a null value. Since <code>char</code> does not
 * support a <code>String</code> conversion, attempting to read a null value
 * as a <code>char</code> must throw a <code>NullPointerException</code>.
 * 
 * @openwire:marshaller code="25"
 * @see javax.jms.Session#createMapMessage()
 * @see javax.jms.BytesMessage
 * @see javax.jms.Message
 * @see javax.jms.ObjectMessage
 * @see javax.jms.StreamMessage
 * @see javax.jms.TextMessage
 */
public class ActiveMQMapMessage extends ActiveMQMessage implements MapMessage {

    public static final byte DATA_STRUCTURE_TYPE = CommandTypes.ACTIVEMQ_MAP_MESSAGE;

    protected transient Map<String, Object> map = new HashMap<String, Object>();

    public Message copy() {
        ActiveMQMapMessage copy = new ActiveMQMapMessage();
        copy(copy);
        return copy;
    }

    private void copy(ActiveMQMapMessage copy) {
        storeContent();
        super.copy(copy);
    }

    // We only need to marshal the content if we are hitting the wire.
    public void beforeMarshall(WireFormat wireFormat) throws IOException {
        super.beforeMarshall(wireFormat);
        storeContent();
    }

    public void clearMarshalledState() throws JMSException {
        super.clearMarshalledState();
        map.clear();
    }

    private void storeContent() {
        try {
            if (getContent() == null && !map.isEmpty()) {
                ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
                OutputStream os = bytesOut;
                ActiveMQConnection connection = getConnection();
                if (connection != null && connection.isUseCompression()) {
                    compressed = true;
                    os = new DeflaterOutputStream(os);
                }
                DataOutputStream dataOut = new DataOutputStream(os);
                MarshallingSupport.marshalPrimitiveMap(map, dataOut);
                dataOut.close();
                setContent(bytesOut.toByteSequence());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Builds the message body from data
     * 
     * @throws JMSException
     * @throws IOException
     */
    private void loadContent() throws JMSException {
        try {
            if (getContent() != null && map.isEmpty()) {
                ByteSequence content = getContent();
                InputStream is = new ByteArrayInputStream(content);
                if (isCompressed()) {
                    is = new InflaterInputStream(is);
                }
                DataInputStream dataIn = new DataInputStream(is);
                map = MarshallingSupport.unmarshalPrimitiveMap(dataIn);
                dataIn.close();
            }
        } catch (IOException e) {
            throw JMSExceptionSupport.create(e);
        }
    }

    public byte getDataStructureType() {
        return DATA_STRUCTURE_TYPE;
    }

    public String getJMSXMimeType() {
        return "jms/map-message";
    }

    /**
     * Clears out the message body. Clearing a message's body does not clear its
     * header values or property entries.
     * <P>
     * If this message body was read-only, calling this method leaves the
     * message body in the same state as an empty body in a newly created
     * message.
     */
    public void clearBody() throws JMSException {
        super.clearBody();
        map.clear();
    }

    /**
     * Returns the <CODE>boolean</CODE> value with the specified name.
     * 
     * @param name the name of the <CODE>boolean</CODE>
     * @return the <CODE>boolean</CODE> value with the specified name
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public boolean getBoolean(String name) throws JMSException {
        initializeReading();
        Object value = map.get(name);
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return ((Boolean)value).booleanValue();
        }
        if (value instanceof String) {
            return Boolean.valueOf(value.toString()).booleanValue();
        } else {
            throw new MessageFormatException(" cannot read a boolean from " + value.getClass().getName());
        }
    }

    /**
     * Returns the <CODE>byte</CODE> value with the specified name.
     * 
     * @param name the name of the <CODE>byte</CODE>
     * @return the <CODE>byte</CODE> value with the specified name
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public byte getByte(String name) throws JMSException {
        initializeReading();
        Object value = map.get(name);
        if (value == null) {
            return 0;
        }
        if (value instanceof Byte) {
            return ((Byte)value).byteValue();
        }
        if (value instanceof String) {
            return Byte.valueOf(value.toString()).byteValue();
        } else {
            throw new MessageFormatException(" cannot read a byte from " + value.getClass().getName());
        }
    }

    /**
     * Returns the <CODE>short</CODE> value with the specified name.
     * 
     * @param name the name of the <CODE>short</CODE>
     * @return the <CODE>short</CODE> value with the specified name
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public short getShort(String name) throws JMSException {
        initializeReading();
        Object value = map.get(name);
        if (value == null) {
            return 0;
        }
        if (value instanceof Short) {
            return ((Short)value).shortValue();
        }
        if (value instanceof Byte) {
            return ((Byte)value).shortValue();
        }
        if (value instanceof String) {
            return Short.valueOf(value.toString()).shortValue();
        } else {
            throw new MessageFormatException(" cannot read a short from " + value.getClass().getName());
        }
    }

    /**
     * Returns the Unicode character value with the specified name.
     * 
     * @param name the name of the Unicode character
     * @return the Unicode character value with the specified name
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public char getChar(String name) throws JMSException {
        initializeReading();
        Object value = map.get(name);
        if (value == null) {
            throw new NullPointerException();
        }
        if (value instanceof Character) {
            return ((Character)value).charValue();
        } else {
            throw new MessageFormatException(" cannot read a short from " + value.getClass().getName());
        }
    }

    /**
     * Returns the <CODE>int</CODE> value with the specified name.
     * 
     * @param name the name of the <CODE>int</CODE>
     * @return the <CODE>int</CODE> value with the specified name
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public int getInt(String name) throws JMSException {
        initializeReading();
        Object value = map.get(name);
        if (value == null) {
            return 0;
        }
        if (value instanceof Integer) {
            return ((Integer)value).intValue();
        }
        if (value instanceof Short) {
            return ((Short)value).intValue();
        }
        if (value instanceof Byte) {
            return ((Byte)value).intValue();
        }
        if (value instanceof String) {
            return Integer.valueOf(value.toString()).intValue();
        } else {
            throw new MessageFormatException(" cannot read an int from " + value.getClass().getName());
        }
    }

    /**
     * Returns the <CODE>long</CODE> value with the specified name.
     * 
     * @param name the name of the <CODE>long</CODE>
     * @return the <CODE>long</CODE> value with the specified name
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public long getLong(String name) throws JMSException {
        initializeReading();
        Object value = map.get(name);
        if (value == null) {
            return 0;
        }
        if (value instanceof Long) {
            return ((Long)value).longValue();
        }
        if (value instanceof Integer) {
            return ((Integer)value).longValue();
        }
        if (value instanceof Short) {
            return ((Short)value).longValue();
        }
        if (value instanceof Byte) {
            return ((Byte)value).longValue();
        }
        if (value instanceof String) {
            return Long.valueOf(value.toString()).longValue();
        } else {
            throw new MessageFormatException(" cannot read a long from " + value.getClass().getName());
        }
    }

    /**
     * Returns the <CODE>float</CODE> value with the specified name.
     * 
     * @param name the name of the <CODE>float</CODE>
     * @return the <CODE>float</CODE> value with the specified name
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public float getFloat(String name) throws JMSException {
        initializeReading();
        Object value = map.get(name);
        if (value == null) {
            return 0;
        }
        if (value instanceof Float) {
            return ((Float)value).floatValue();
        }
        if (value instanceof String) {
            return Float.valueOf(value.toString()).floatValue();
        } else {
            throw new MessageFormatException(" cannot read a float from " + value.getClass().getName());
        }
    }

    /**
     * Returns the <CODE>double</CODE> value with the specified name.
     * 
     * @param name the name of the <CODE>double</CODE>
     * @return the <CODE>double</CODE> value with the specified name
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public double getDouble(String name) throws JMSException {
        initializeReading();
        Object value = map.get(name);
        if (value == null) {
            return 0;
        }
        if (value instanceof Double) {
            return ((Double)value).doubleValue();
        }
        if (value instanceof Float) {
            return ((Float)value).floatValue();
        }
        if (value instanceof String) {
            return Float.valueOf(value.toString()).floatValue();
        } else {
            throw new MessageFormatException(" cannot read a double from " + value.getClass().getName());
        }
    }

    /**
     * Returns the <CODE>String</CODE> value with the specified name.
     * 
     * @param name the name of the <CODE>String</CODE>
     * @return the <CODE>String</CODE> value with the specified name; if there
     *         is no item by this name, a null value is returned
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public String getString(String name) throws JMSException {
        initializeReading();
        Object value = map.get(name);
        if (value == null) {
            return null;
        }
        if (value instanceof byte[]) {
            throw new MessageFormatException("Use getBytes to read a byte array");
        } else {
            return value.toString();
        }
    }

    /**
     * Returns the byte array value with the specified name.
     * 
     * @param name the name of the byte array
     * @return a copy of the byte array value with the specified name; if there
     *         is no item by this name, a null value is returned.
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     * @throws MessageFormatException if this type conversion is invalid.
     */
    public byte[] getBytes(String name) throws JMSException {
        initializeReading();
        Object value = map.get(name);
        if (value instanceof byte[]) {
            return (byte[])value;
        } else {
            throw new MessageFormatException(" cannot read a byte[] from " + value.getClass().getName());
        }
    }

    /**
     * Returns the value of the object with the specified name.
     * <P>
     * This method can be used to return, in objectified format, an object in
     * the Java programming language ("Java object") that had been stored in the
     * Map with the equivalent <CODE>setObject</CODE> method call, or its
     * equivalent primitive <CODE>set <I>type </I></CODE> method.
     * <P>
     * Note that byte values are returned as <CODE>byte[]</CODE>, not
     * <CODE>Byte[]</CODE>.
     * 
     * @param name the name of the Java object
     * @return a copy of the Java object value with the specified name, in
     *         objectified format (for example, if the object was set as an
     *         <CODE>int</CODE>, an <CODE>Integer</CODE> is returned); if
     *         there is no item by this name, a null value is returned
     * @throws JMSException if the JMS provider fails to read the message due to
     *                 some internal error.
     */
    public Object getObject(String name) throws JMSException {
        initializeReading();
        return map.get(name);
    }

    /**
     * Returns an <CODE>Enumeration</CODE> of all the names in the
     * <CODE>MapMessage</CODE> object.
     * 
     * @return an enumeration of all the names in this <CODE>MapMessage</CODE>
     * @throws JMSException
     */
    public Enumeration<String> getMapNames() throws JMSException {
        initializeReading();
        return Collections.enumeration(map.keySet());
    }

    protected void put(String name, Object value) throws JMSException {
        if (name == null) {
            throw new IllegalArgumentException("The name of the property cannot be null.");
        }
        if (name.length() == 0) {
            throw new IllegalArgumentException("The name of the property cannot be an emprty string.");
        }
        map.put(name, value);
    }

    /**
     * Sets a <CODE>boolean</CODE> value with the specified name into the Map.
     * 
     * @param name the name of the <CODE>boolean</CODE>
     * @param value the <CODE>boolean</CODE> value to set in the Map
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setBoolean(String name, boolean value) throws JMSException {
        initializeWriting();
        put(name, value ? Boolean.TRUE : Boolean.FALSE);
    }

    /**
     * Sets a <CODE>byte</CODE> value with the specified name into the Map.
     * 
     * @param name the name of the <CODE>byte</CODE>
     * @param value the <CODE>byte</CODE> value to set in the Map
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setByte(String name, byte value) throws JMSException {
        initializeWriting();
        put(name, Byte.valueOf(value));
    }

    /**
     * Sets a <CODE>short</CODE> value with the specified name into the Map.
     * 
     * @param name the name of the <CODE>short</CODE>
     * @param value the <CODE>short</CODE> value to set in the Map
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setShort(String name, short value) throws JMSException {
        initializeWriting();
        put(name, Short.valueOf(value));
    }

    /**
     * Sets a Unicode character value with the specified name into the Map.
     * 
     * @param name the name of the Unicode character
     * @param value the Unicode character value to set in the Map
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setChar(String name, char value) throws JMSException {
        initializeWriting();
        put(name, Character.valueOf(value));
    }

    /**
     * Sets an <CODE>int</CODE> value with the specified name into the Map.
     * 
     * @param name the name of the <CODE>int</CODE>
     * @param value the <CODE>int</CODE> value to set in the Map
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setInt(String name, int value) throws JMSException {
        initializeWriting();
        put(name, Integer.valueOf(value));
    }

    /**
     * Sets a <CODE>long</CODE> value with the specified name into the Map.
     * 
     * @param name the name of the <CODE>long</CODE>
     * @param value the <CODE>long</CODE> value to set in the Map
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setLong(String name, long value) throws JMSException {
        initializeWriting();
        put(name, Long.valueOf(value));
    }

    /**
     * Sets a <CODE>float</CODE> value with the specified name into the Map.
     * 
     * @param name the name of the <CODE>float</CODE>
     * @param value the <CODE>float</CODE> value to set in the Map
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setFloat(String name, float value) throws JMSException {
        initializeWriting();
        put(name, new Float(value));
    }

    /**
     * Sets a <CODE>double</CODE> value with the specified name into the Map.
     * 
     * @param name the name of the <CODE>double</CODE>
     * @param value the <CODE>double</CODE> value to set in the Map
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setDouble(String name, double value) throws JMSException {
        initializeWriting();
        put(name, new Double(value));
    }

    /**
     * Sets a <CODE>String</CODE> value with the specified name into the Map.
     * 
     * @param name the name of the <CODE>String</CODE>
     * @param value the <CODE>String</CODE> value to set in the Map
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setString(String name, String value) throws JMSException {
        initializeWriting();
        put(name, value);
    }

    /**
     * Sets a byte array value with the specified name into the Map.
     * 
     * @param name the name of the byte array
     * @param value the byte array value to set in the Map; the array is copied
     *                so that the value for <CODE>name </CODE> will not be
     *                altered by future modifications
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws NullPointerException if the name is null, or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setBytes(String name, byte[] value) throws JMSException {
        initializeWriting();
        if (value != null) {
            put(name, value);
        } else {
            map.remove(name);
        }
    }

    /**
     * Sets a portion of the byte array value with the specified name into the
     * Map.
     * 
     * @param name the name of the byte array
     * @param value the byte array value to set in the Map
     * @param offset the initial offset within the byte array
     * @param length the number of bytes to use
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setBytes(String name, byte[] value, int offset, int length) throws JMSException {
        initializeWriting();
        byte[] data = new byte[length];
        System.arraycopy(value, offset, data, 0, length);
        put(name, data);
    }

    /**
     * Sets an object value with the specified name into the Map.
     * <P>
     * This method works only for the objectified primitive object types (<code>Integer</code>,<code>Double</code>,
     * <code>Long</code> &nbsp;...), <code>String</code> objects, and byte
     * arrays.
     * 
     * @param name the name of the Java object
     * @param value the Java object value to set in the Map
     * @throws JMSException if the JMS provider fails to write the message due
     *                 to some internal error.
     * @throws IllegalArgumentException if the name is null or if the name is an
     *                 empty string.
     * @throws MessageFormatException if the object is invalid.
     * @throws MessageNotWriteableException if the message is in read-only mode.
     */
    public void setObject(String name, Object value) throws JMSException {
        initializeWriting();
        if (value != null) {
            // byte[] not allowed on properties
            if (!(value instanceof byte[])) {
                checkValidObject(value);
            }
            put(name, value);
        } else {
            put(name, null);
        }
    }

    /**
     * Indicates whether an item exists in this <CODE>MapMessage</CODE>
     * object.
     * 
     * @param name the name of the item to test
     * @return true if the item exists
     * @throws JMSException if the JMS provider fails to determine if the item
     *                 exists due to some internal error.
     */
    public boolean itemExists(String name) throws JMSException {
        initializeReading();
        return map.containsKey(name);
    }

    private void initializeReading() throws JMSException {
        loadContent();
    }

    private void initializeWriting() throws MessageNotWriteableException {
        checkReadOnlyBody();
        setContent(null);
    }

    public String toString() {
        return super.toString() + " ActiveMQMapMessage{ " + "theTable = " + map + " }";
    }

    public Map<String, Object> getContentMap() throws JMSException {
        initializeReading();
        return map;
    }
}
