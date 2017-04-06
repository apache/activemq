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
package org.apache.activemq.util;

import java.io.DataInput;
import java.io.DataInputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UTFDataFormatException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.fusesource.hawtbuf.UTF8Buffer;

/**
 * The fixed version of the UTF8 encoding function. Some older JVM's UTF8
 * encoding function breaks when handling large strings.
 */
public final class MarshallingSupport {

    public static final byte NULL = 0;
    public static final byte BOOLEAN_TYPE = 1;
    public static final byte BYTE_TYPE = 2;
    public static final byte CHAR_TYPE = 3;
    public static final byte SHORT_TYPE = 4;
    public static final byte INTEGER_TYPE = 5;
    public static final byte LONG_TYPE = 6;
    public static final byte DOUBLE_TYPE = 7;
    public static final byte FLOAT_TYPE = 8;
    public static final byte STRING_TYPE = 9;
    public static final byte BYTE_ARRAY_TYPE = 10;
    public static final byte MAP_TYPE = 11;
    public static final byte LIST_TYPE = 12;
    public static final byte BIG_STRING_TYPE = 13;

    private MarshallingSupport() {}

    public static void marshalPrimitiveMap(Map<String, Object> map, DataOutputStream out) throws IOException {
        if (map == null) {
            out.writeInt(-1);
        } else {
            out.writeInt(map.size());
            for (String name : map.keySet()) {
                out.writeUTF(name);
                Object value = map.get(name);
                marshalPrimitive(out, value);
            }
        }
    }

    public static Map<String, Object> unmarshalPrimitiveMap(DataInputStream in) throws IOException {
        return unmarshalPrimitiveMap(in, Integer.MAX_VALUE);
    }

    public static Map<String, Object> unmarshalPrimitiveMap(DataInputStream in, boolean force) throws IOException {
        return unmarshalPrimitiveMap(in, Integer.MAX_VALUE, force);
    }

    public static Map<String, Object> unmarshalPrimitiveMap(DataInputStream in, int maxPropertySize) throws IOException {
        return unmarshalPrimitiveMap(in, maxPropertySize, false);
    }

    /**
     * @param in
     * @return
     * @throws IOException
     * @throws IOException
     */
    public static Map<String, Object> unmarshalPrimitiveMap(DataInputStream in, int maxPropertySize, boolean force) throws IOException {
        int size = in.readInt();
        if (size > maxPropertySize) {
            throw new IOException("Primitive map is larger than the allowed size: " + size);
        }
        if (size < 0) {
            return null;
        } else {
            Map<String, Object> rc = new HashMap<String, Object>(size);
            for (int i = 0; i < size; i++) {
                String name = in.readUTF();
                rc.put(name, unmarshalPrimitive(in, force));
            }
            return rc;
        }
    }

    public static void marshalPrimitiveList(List<Object> list, DataOutputStream out) throws IOException {
        out.writeInt(list.size());
        for (Object element : list) {
            marshalPrimitive(out, element);
        }
    }

    public static List<Object> unmarshalPrimitiveList(DataInputStream in) throws IOException {
        return unmarshalPrimitiveList(in, false);
    }

    public static List<Object> unmarshalPrimitiveList(DataInputStream in, boolean force) throws IOException {
        int size = in.readInt();
        List<Object> answer = new ArrayList<Object>(size);
        while (size-- > 0) {
            answer.add(unmarshalPrimitive(in, force));
        }
        return answer;
    }

    public static void marshalPrimitive(DataOutputStream out, Object value) throws IOException {
        if (value == null) {
            marshalNull(out);
        } else if (value.getClass() == Boolean.class) {
            marshalBoolean(out, ((Boolean)value).booleanValue());
        } else if (value.getClass() == Byte.class) {
            marshalByte(out, ((Byte)value).byteValue());
        } else if (value.getClass() == Character.class) {
            marshalChar(out, ((Character)value).charValue());
        } else if (value.getClass() == Short.class) {
            marshalShort(out, ((Short)value).shortValue());
        } else if (value.getClass() == Integer.class) {
            marshalInt(out, ((Integer)value).intValue());
        } else if (value.getClass() == Long.class) {
            marshalLong(out, ((Long)value).longValue());
        } else if (value.getClass() == Float.class) {
            marshalFloat(out, ((Float)value).floatValue());
        } else if (value.getClass() == Double.class) {
            marshalDouble(out, ((Double)value).doubleValue());
        } else if (value.getClass() == byte[].class) {
            marshalByteArray(out, (byte[])value);
        } else if (value.getClass() == String.class) {
            marshalString(out, (String)value);
        } else  if (value.getClass() == UTF8Buffer.class) {
            marshalString(out, value.toString());
        } else if (value instanceof Map) {
            out.writeByte(MAP_TYPE);
            marshalPrimitiveMap((Map<String, Object>)value, out);
        } else if (value instanceof List) {
            out.writeByte(LIST_TYPE);
            marshalPrimitiveList((List<Object>)value, out);
        } else {
            throw new IOException("Object is not a primitive: " + value);
        }
    }

    public static Object unmarshalPrimitive(DataInputStream in) throws IOException {
        return unmarshalPrimitive(in, false);
    }

    public static Object unmarshalPrimitive(DataInputStream in, boolean force) throws IOException {
        Object value = null;
        byte type = in.readByte();
        switch (type) {
        case BYTE_TYPE:
            value = Byte.valueOf(in.readByte());
            break;
        case BOOLEAN_TYPE:
            value = in.readBoolean() ? Boolean.TRUE : Boolean.FALSE;
            break;
        case CHAR_TYPE:
            value = Character.valueOf(in.readChar());
            break;
        case SHORT_TYPE:
            value = Short.valueOf(in.readShort());
            break;
        case INTEGER_TYPE:
            value = Integer.valueOf(in.readInt());
            break;
        case LONG_TYPE:
            value = Long.valueOf(in.readLong());
            break;
        case FLOAT_TYPE:
            value = new Float(in.readFloat());
            break;
        case DOUBLE_TYPE:
            value = new Double(in.readDouble());
            break;
        case BYTE_ARRAY_TYPE:
            value = new byte[in.readInt()];
            in.readFully((byte[])value);
            break;
        case STRING_TYPE:
            if (force) {
                value = in.readUTF();
            } else {
                value = readUTF(in, in.readUnsignedShort());
            }
            break;
        case BIG_STRING_TYPE: {
            if (force) {
                value = readUTF8(in);
            } else {
                value = readUTF(in, in.readInt());
            }
            break;
        }
        case MAP_TYPE:
            value = unmarshalPrimitiveMap(in, true);
            break;
        case LIST_TYPE:
            value = unmarshalPrimitiveList(in, true);
            break;
        case NULL:
            value = null;
            break;
        default:
            throw new IOException("Unknown primitive type: " + type);
        }
        return value;
    }

    public static UTF8Buffer readUTF(DataInputStream in, int length) throws IOException {
        byte data[] = new byte[length];
        in.readFully(data);
        return new UTF8Buffer(data);
    }

    public static void marshalNull(DataOutputStream out) throws IOException {
        out.writeByte(NULL);
    }

    public static void marshalBoolean(DataOutputStream out, boolean value) throws IOException {
        out.writeByte(BOOLEAN_TYPE);
        out.writeBoolean(value);
    }

    public static void marshalByte(DataOutputStream out, byte value) throws IOException {
        out.writeByte(BYTE_TYPE);
        out.writeByte(value);
    }

    public static void marshalChar(DataOutputStream out, char value) throws IOException {
        out.writeByte(CHAR_TYPE);
        out.writeChar(value);
    }

    public static void marshalShort(DataOutputStream out, short value) throws IOException {
        out.writeByte(SHORT_TYPE);
        out.writeShort(value);
    }

    public static void marshalInt(DataOutputStream out, int value) throws IOException {
        out.writeByte(INTEGER_TYPE);
        out.writeInt(value);
    }

    public static void marshalLong(DataOutputStream out, long value) throws IOException {
        out.writeByte(LONG_TYPE);
        out.writeLong(value);
    }

    public static void marshalFloat(DataOutputStream out, float value) throws IOException {
        out.writeByte(FLOAT_TYPE);
        out.writeFloat(value);
    }

    public static void marshalDouble(DataOutputStream out, double value) throws IOException {
        out.writeByte(DOUBLE_TYPE);
        out.writeDouble(value);
    }

    public static void marshalByteArray(DataOutputStream out, byte[] value) throws IOException {
        marshalByteArray(out, value, 0, value.length);
    }

    public static void marshalByteArray(DataOutputStream out, byte[] value, int offset, int length) throws IOException {
        out.writeByte(BYTE_ARRAY_TYPE);
        out.writeInt(length);
        out.write(value, offset, length);
    }

    public static void marshalString(DataOutputStream out, String s) throws IOException {
        // If it's too big, out.writeUTF may not able able to write it out.
        if (s.length() < Short.MAX_VALUE / 4) {
            out.writeByte(STRING_TYPE);
            out.writeUTF(s);
        } else {
            out.writeByte(BIG_STRING_TYPE);
            writeUTF8(out, s);
        }
    }

    public static void writeUTF8(DataOutput dataOut, String text) throws IOException {
        if (text != null) {
            long utfCount = countUTFBytes(text);
            dataOut.writeInt((int)utfCount);

            byte[] buffer = new byte[(int)utfCount];
            int len = writeUTFBytesToBuffer(text, (int) utfCount, buffer, 0);
            dataOut.write(buffer, 0, len);

            assert utfCount==len;
        } else {
            dataOut.writeInt(-1);
        }
    }

    /**
     * From: http://svn.apache.org/repos/asf/harmony/enhanced/java/trunk/classlib/modules/luni/src/main/java/java/io/DataOutputStream.java
     */
    public static long countUTFBytes(String str) {
        int utfCount = 0, length = str.length();
        for (int i = 0; i < length; i++) {
            int charValue = str.charAt(i);
            if (charValue > 0 && charValue <= 127) {
                utfCount++;
            } else if (charValue <= 2047) {
                utfCount += 2;
            } else {
                utfCount += 3;
            }
        }
        return utfCount;
    }

    /**
     * From: http://svn.apache.org/repos/asf/harmony/enhanced/java/trunk/classlib/modules/luni/src/main/java/java/io/DataOutputStream.java
     */
    public static int writeUTFBytesToBuffer(String str, long count,
                                     byte[] buffer, int offset) throws IOException {
        int length = str.length();
        for (int i = 0; i < length; i++) {
            int charValue = str.charAt(i);
            if (charValue > 0 && charValue <= 127) {
                buffer[offset++] = (byte) charValue;
            } else if (charValue <= 2047) {
                buffer[offset++] = (byte) (0xc0 | (0x1f & (charValue >> 6)));
                buffer[offset++] = (byte) (0x80 | (0x3f & charValue));
            } else {
                buffer[offset++] = (byte) (0xe0 | (0x0f & (charValue >> 12)));
                buffer[offset++] = (byte) (0x80 | (0x3f & (charValue >> 6)));
                buffer[offset++] = (byte) (0x80 | (0x3f & charValue));
            }
        }
        return offset;
    }

    public static String readUTF8(DataInput dataIn) throws IOException {
        int utflen = dataIn.readInt();
        if (utflen > -1) {
            byte bytearr[] = new byte[utflen];
            char chararr[] = new char[utflen];
            dataIn.readFully(bytearr, 0, utflen);
            return convertUTF8WithBuf(bytearr, chararr, 0, utflen);
        } else {
            return null;
        }
    }

    /**
     * From: http://svn.apache.org/repos/asf/harmony/enhanced/java/trunk/classlib/modules/luni/src/main/java/org/apache/harmony/luni/util/Util.java
     */
    public static String convertUTF8WithBuf(byte[] buf, char[] out, int offset,
                                            int utfSize) throws UTFDataFormatException {
        int count = 0, s = 0, a;
        while (count < utfSize) {
            if ((out[s] = (char) buf[offset + count++]) < '\u0080')
                s++;
            else if (((a = out[s]) & 0xe0) == 0xc0) {
                if (count >= utfSize)
                    throw new UTFDataFormatException();
                int b = buf[offset + count++];
                if ((b & 0xC0) != 0x80)
                    throw new UTFDataFormatException();
                out[s++] = (char) (((a & 0x1F) << 6) | (b & 0x3F));
            } else if ((a & 0xf0) == 0xe0) {
                if (count + 1 >= utfSize)
                    throw new UTFDataFormatException();
                int b = buf[offset + count++];
                int c = buf[offset + count++];
                if (((b & 0xC0) != 0x80) || ((c & 0xC0) != 0x80))
                    throw new UTFDataFormatException();
                out[s++] = (char) (((a & 0x0F) << 12) | ((b & 0x3F) << 6) | (c & 0x3F));
            } else {
                throw new UTFDataFormatException();
            }
        }
        return new String(out, 0, s);
    }

    public static String propertiesToString(Properties props) throws IOException {
        String result = "";
        if (props != null) {
            DataByteArrayOutputStream dataOut = new DataByteArrayOutputStream();
            props.store(dataOut, "");
            result = new String(dataOut.getData(), 0, dataOut.size());
            dataOut.close();
        }
        return result;
    }

    public static Properties stringToProperties(String str) throws IOException {
        Properties result = new Properties();
        if (str != null && str.length() > 0) {
            DataByteArrayInputStream dataIn = new DataByteArrayInputStream(str.getBytes());
            result.load(dataIn);
            dataIn.close();
        }
        return result;
    }

    public static String truncate64(String text) {
        if (text.length() > 63) {
            text = text.substring(0, 45) + "..." + text.substring(text.length() - 12);
        }
        return text;
    }
}
