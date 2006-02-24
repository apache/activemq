/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using System;
using System.IO;
using System.Net;
using System.Text;

using OpenWire.Client.Commands;
using OpenWire.Client.Core;
using OpenWire.Client.IO;
using System.Collections;

namespace OpenWire.Client.Core
{
    /// <summary>
    /// A base class with useful implementation inheritence methods
    /// for creating marshallers of the OpenWire protocol
    /// </summary>
    public abstract class DataStreamMarshaller
    {
        public const byte NULL                    = 0;
        public const byte BOOLEAN_TYPE            = 1;
        public const byte BYTE_TYPE               = 2;
        public const byte CHAR_TYPE               = 3;
        public const byte SHORT_TYPE              = 4;
        public const byte INTEGER_TYPE            = 5;
        public const byte LONG_TYPE               = 6;
        public const byte DOUBLE_TYPE             = 7;
        public const byte FLOAT_TYPE              = 8;
        public const byte STRING_TYPE             = 9;
        public const byte BYTE_ARRAY_TYPE         = 10;
        
        private static String[] HEX_TABLE = new String[]{
            "00", "01", "02", "03", "04", "05", "06", "07", "08", "09", "0a", "0b", "0c", "0d", "0e", "0f",
            "10", "11", "12", "13", "14", "15", "16", "17", "18", "19", "1a", "1b", "1c", "1d", "1e", "1f",
            "20", "21", "22", "23", "24", "25", "26", "27", "28", "29", "2a", "2b", "2c", "2d", "2e", "2f",
            "30", "31", "32", "33", "34", "35", "36", "37", "38", "39", "3a", "3b", "3c", "3d", "3e", "3f",
            "40", "41", "42", "43", "44", "45", "46", "47", "48", "49", "4a", "4b", "4c", "4d", "4e", "4f",
            "50", "51", "52", "53", "54", "55", "56", "57", "58", "59", "5a", "5b", "5c", "5d", "5e", "5f",
            "60", "61", "62", "63", "64", "65", "66", "67", "68", "69", "6a", "6b", "6c", "6d", "6e", "6f",
            "70", "71", "72", "73", "74", "75", "76", "77", "78", "79", "7a", "7b", "7c", "7d", "7e", "7f",
            "80", "81", "82", "83", "84", "85", "86", "87", "88", "89", "8a", "8b", "8c", "8d", "8e", "8f",
            "90", "91", "92", "93", "94", "95", "96", "97", "98", "99", "9a", "9b", "9c", "9d", "9e", "9f",
            "a0", "a1", "a2", "a3", "a4", "a5", "a6", "a7", "a8", "a9", "aa", "ab", "ac", "ad", "ae", "af",
            "b0", "b1", "b2", "b3", "b4", "b5", "b6", "b7", "b8", "b9", "ba", "bb", "bc", "bd", "be", "bf",
            "c0", "c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8", "c9", "ca", "cb", "cc", "cd", "ce", "cf",
            "d0", "d1", "d2", "d3", "d4", "d5", "d6", "d7", "d8", "d9", "da", "db", "dc", "dd", "de", "df",
            "e0", "e1", "e2", "e3", "e4", "e5", "e6", "e7", "e8", "e9", "ea", "eb", "ec", "ed", "ee", "ef",
            "f0", "f1", "f2", "f3", "f4", "f5", "f6", "f7", "f8", "f9", "fa", "fb", "fc", "fd", "fe", "ff",
        };
        
        public abstract DataStructure CreateObject();
        public abstract byte GetDataStructureType();
        
        public virtual int Marshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs)
        {
            return 0;
        }
        public virtual void Marshal2(
            OpenWireFormat wireFormat,
            Object o,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
        }
        
        public virtual void Unmarshal(
            OpenWireFormat wireFormat,
            Object o,
            BinaryReader dataIn,
            BooleanStream bs)
        {
        }
        
        
        protected virtual DataStructure UnmarshalNestedObject(
            OpenWireFormat wireFormat,
            BinaryReader dataIn,
            BooleanStream bs)
        {
            return wireFormat.UnmarshalNestedObject(dataIn, bs);
        }
        
        protected virtual int Marshal1NestedObject(
            OpenWireFormat wireFormat,
            DataStructure o,
            BooleanStream bs)
        {
            return wireFormat.Marshal1NestedObject(o, bs);
        }
        
        protected virtual void Marshal2NestedObject(
            OpenWireFormat wireFormat,
            DataStructure o,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
            wireFormat.Marshal2NestedObject(o, dataOut, bs);
        }
        
        protected virtual DataStructure UnmarshalCachedObject(
            OpenWireFormat wireFormat,
            BinaryReader dataIn,
            BooleanStream bs)
        {
            /*
             if (wireFormat.isCacheEnabled()) {
             if (bs.ReadBoolean()) {
             short index = dataInReadShort(dataIn)Int16();
             DataStructure value = wireFormat.UnmarshalNestedObject(dataIn, bs);
             wireFormat.setInUnmarshallCache(index, value);
             return value;
             } else {
             short index = ReadShort(dataIn);
             return wireFormat.getFromUnmarshallCache(index);
             }
             } else {
             return wireFormat.UnmarshalNestedObject(dataIn, bs);
             }
             */
            return wireFormat.UnmarshalNestedObject(dataIn, bs);
        }
        
        protected virtual int Marshal1CachedObject(
            OpenWireFormat wireFormat,
            DataStructure o,
            BooleanStream bs)
        {
            /*
             if (wireFormat.isCacheEnabled()) {
             Short index = wireFormat.getMarshallCacheIndex(o);
             bs.WriteBoolean(index == null);
             if (index == null) {
             int rc = wireFormat.Marshal1NestedObject(o, bs);
             wireFormat.addToMarshallCache(o);
             return 2 + rc;
             } else {
             return 2;
             }
             } else {
             return wireFormat.Marshal1NestedObject(o, bs);
             }
             */
            return wireFormat.Marshal1NestedObject(o, bs);
        }
        
        protected virtual void Marshal2CachedObject(
            OpenWireFormat wireFormat,
            DataStructure o,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
            /*
             if (wireFormat.isCacheEnabled()) {
             Short index = wireFormat.getMarshallCacheIndex(o);
             if (bs.ReadBoolean()) {
             WriteShort(index.shortValue(), dataOut);
             wireFormat.Marshal2NestedObject(o, dataOut, bs);
             } else {
             WriteShort(index.shortValue(), dataOut);
             }
             } else {
             wireFormat.Marshal2NestedObject(o, dataOut, bs);
             }
             */
            wireFormat.Marshal2NestedObject(o, dataOut, bs);
        }
        
        
        
        protected virtual String ReadString(BinaryReader dataIn, BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                if (bs.ReadBoolean())
                {
                    return ReadAsciiString(dataIn);
                }
                else
                {
                    return ReadUTF8(dataIn);
                }
            }
            else
            {
                return null;
            }
        }
        
        protected virtual String ReadAsciiString(BinaryReader dataIn)
        {
            int size = ReadShort(dataIn);
            byte[] data = new byte[size];
            dataIn.Read(data, 0, size);
            char[] text = new char[size];
            for (int i = 0; i < size; i++)
            {
                text[i] = (char) data[i];
            }
            return new String(text);
        }
        
        protected virtual int WriteString(String value, BooleanStream bs)
        {
            bs.WriteBoolean(value != null);
            if (value != null)
            {
                int strlen = value.Length;
                
                // TODO until we get UTF8 working, lets just force ASCII
                bs.WriteBoolean(true);
                return strlen + 2;
                
                
                /*
                 int utflen = 0;
                 int c = 0;
                 bool isOnlyAscii = true;
                 char[] charr = value.ToCharArray();
                 for (int i = 0; i < strlen; i++)
                 {
                 c = charr[i];
                 if ((c >= 0x0001) && (c <= 0x007F))
                 {
                 utflen++;
                 }
                 else if (c > 0x07FF)
                 {
                 utflen += 3;
                 isOnlyAscii = false;
                 }
                 else
                 {
                 isOnlyAscii = false;
                 utflen += 2;
                 }
                 }
                 
                 if (utflen >= Int16.MaxValue)
                 throw new IOException("Encountered a String value that is too long to encode.");
                 
                 bs.WriteBoolean(isOnlyAscii);
                 return utflen + 2;
                 */
            }
            else
            {
                return 0;
            }
        }
        
        public static void WriteString(String value, BinaryWriter dataOut, BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                // If we verified it only holds ascii values
                if (bs.ReadBoolean())
                {
                    WriteShort((short) value.Length, dataOut);
                    // now lets write the bytes
                    char[] chars = value.ToCharArray();
                    for (int i = 0; i < chars.Length; i++)
                    {
                        WriteByte((byte) chars[i], dataOut);
                    }
                }
                else
                {
                    // TODO how should we properly write a String so that Java will grok it???
                    dataOut.Write(value);
                }
            }
        }
        
        public static byte ReadByte(BinaryReader dataIn)
        {
            return dataIn.ReadByte();
        }
        
        public static char ReadChar(BinaryReader dataIn)
        {
            return (char) ReadShort(dataIn);
        }
        
        public static short ReadShort(BinaryReader dataIn)
        {
            return SwitchEndian(dataIn.ReadInt16());
        }
        
        public static int ReadInt(BinaryReader dataIn)
        {
            return SwitchEndian(dataIn.ReadInt32());
        }
        
        public static long ReadLong(BinaryReader dataIn)
        {
            return SwitchEndian(dataIn.ReadInt64());
        }
        
        public static void WriteByte(byte value, BinaryWriter dataOut)
        {
            dataOut.Write(value);
        }
        
        public static void WriteChar(char value, BinaryWriter dataOut)
        {
            dataOut.Write(SwitchEndian((short) value));
        }
        
        public static void WriteShort(short value, BinaryWriter dataOut)
        {
            dataOut.Write(SwitchEndian(value));
        }
        
        public static void WriteInt(int value, BinaryWriter dataOut)
        {
            dataOut.Write(SwitchEndian(value));
        }
        
        public static void WriteLong(long value, BinaryWriter dataOut)
        {
            dataOut.Write(SwitchEndian(value));
        }
        
        
        /// <summary>
        /// Switches from one endian to the other
        /// </summary>
        /// <param name="value">An int</param>
        /// <returns>An int</retutns>
        public static int SwitchEndian(int x)
        {
            return ((x << 24) | ((x & 0xff00) << 8) | ((x & 0xff0000) >> 8) | (x >> 24));
        }
        
        public static short SwitchEndian(short x)
        {
            int low = x & 0xff;
            int high = x & 0xff00;
            return(short)(high >> 8 | low << 8);
        }
        
        public static long SwitchEndian(long x)
        {
            long answer = 0;
            for (int i = 0; i < 8; i++)
            {
                long lowest = x & 0xff;
                x >>= 8;
                answer <<= 8;
                answer += lowest;
            }
            return answer;
        }
        
        public virtual int Marshal1Long(OpenWireFormat wireFormat, long o, BooleanStream bs)
        {
            if (o == 0L)
            {
                bs.WriteBoolean(false);
                bs.WriteBoolean(false);
                return 0;
            }
            else
            {
                ulong ul = (ulong) o;
                if ((ul & 0xFFFFFFFFFFFF0000ul) == 0L)
                {
                    bs.WriteBoolean(false);
                    bs.WriteBoolean(true);
                    return 2;
                }
                else if ((ul & 0xFFFFFFFF00000000ul) == 0L)
                {
                    bs.WriteBoolean(true);
                    bs.WriteBoolean(false);
                    return 4;
                }
                else
                {
                    bs.WriteBoolean(true);
                    bs.WriteBoolean(true);
                    return 8;
                }
            }
        }
        
        public virtual void Marshal2Long(
            OpenWireFormat wireFormat,
            long o,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                if (bs.ReadBoolean())
                {
                    WriteLong(o, dataOut);
                }
                else
                {
                    WriteInt((int) o, dataOut);
                }
            }
            else
            {
                if (bs.ReadBoolean())
                {
                    WriteShort((short) o, dataOut);
                }
            }
        }
        public virtual long UnmarshalLong(OpenWireFormat wireFormat, BinaryReader dataIn, BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                if (bs.ReadBoolean())
                {
                    return ReadLong(dataIn);
                }
                else
                {
                    return ReadInt(dataIn);
                }
            }
            else
            {
                if (bs.ReadBoolean())
                {
                    return ReadShort(dataIn);
                }
                else
                {
                    return 0;
                }
            }
        }
        protected virtual int MarshalObjectArray(
            OpenWireFormat wireFormat,
            DataStructure[] objects,
            BooleanStream bs)
        {
            if (objects != null)
            {
                int rc = 0;
                bs.WriteBoolean(true);
                rc += 2;
                for (int i = 0; i < objects.Length; i++)
                {
                    rc += Marshal1NestedObject(wireFormat, objects[i], bs);
                }
                return rc;
            }
            else
            {
                bs.WriteBoolean(false);
                return 0;
            }
        }
        
        protected virtual void MarshalObjectArray(
            OpenWireFormat wireFormat,
            DataStructure[] objects,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                WriteShort((short) objects.Length, dataOut);
                for (int i = 0; i < objects.Length; i++)
                {
                    Marshal2NestedObject(wireFormat, objects[i], dataOut, bs);
                }
            }
        }
        
        protected virtual byte[] ReadBytes(BinaryReader dataIn, bool flag)
        {
            if (flag)
            {
                int size = ReadInt(dataIn);
                return dataIn.ReadBytes(size);
            }
            else
            {
                return null;
            }
        }
        
        protected virtual byte[] ReadBytes(BinaryReader dataIn)
        {
            int size = ReadInt(dataIn);
            return dataIn.ReadBytes(size);
        }
        
        protected virtual byte[] ReadBytes(BinaryReader dataIn, int size)
        {
            return dataIn.ReadBytes(size);
        }
        
        protected virtual void WriteBytes(byte[] command, BinaryWriter dataOut)
        {
            WriteInt(command.Length, dataOut);
            dataOut.Write(command);
        }
        
        protected virtual BrokerError UnmarshalBrokerError(
            OpenWireFormat wireFormat,
            BinaryReader dataIn,
            BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                BrokerError answer = new BrokerError();
                
                answer.ExceptionClass = ReadString(dataIn, bs);
                answer.Message = ReadString(dataIn, bs);
                if (wireFormat.StackTraceEnabled)
                {
                    short length = ReadShort(dataIn);
                    StackTraceElement[] stackTrace = new StackTraceElement[length];
                    for (int i = 0; i < stackTrace.Length; i++)
                    {
                        StackTraceElement element = new StackTraceElement();
                        element.ClassName = ReadString(dataIn, bs);
                        element.MethodName = ReadString(dataIn, bs);
                        element.FileName = ReadString(dataIn, bs);
                        element.LineNumber = ReadInt(dataIn);
                        stackTrace[i] = element;
                    }
                    answer.StackTraceElements = stackTrace;
                    answer.Cause = UnmarshalBrokerError(wireFormat, dataIn, bs);
                }
                return answer;
            }
            else
            {
                return null;
            }
        }
        
        protected int MarshalBrokerError(OpenWireFormat wireFormat, BrokerError o, BooleanStream bs)
        {
            if (o == null)
            {
                bs.WriteBoolean(false);
                return 0;
            }
            else
            {
                int rc = 0;
                bs.WriteBoolean(true);
                rc += WriteString(o.ExceptionClass, bs);
                rc += WriteString(o.Message, bs);
                if (wireFormat.StackTraceEnabled)
                {
                    rc += 2;
                    StackTraceElement[] stackTrace = o.StackTraceElements;
                    for (int i = 0; i < stackTrace.Length; i++)
                    {
                        StackTraceElement element = stackTrace[i];
                        rc += WriteString(element.ClassName, bs);
                        rc += WriteString(element.MethodName, bs);
                        rc += WriteString(element.FileName, bs);
                        rc += 4;
                    }
                    rc += MarshalBrokerError(wireFormat, o.Cause, bs);
                }
                
                return rc;
            }
        }
        
        protected void MarshalBrokerError(
            OpenWireFormat wireFormat,
            BrokerError o,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                WriteString(o.ExceptionClass, dataOut, bs);
                WriteString(o.Message, dataOut, bs);
                if (wireFormat.StackTraceEnabled)
                {
                    StackTraceElement[] stackTrace = o.StackTraceElements;
                    WriteShort((short) stackTrace.Length, dataOut);
                    
                    for (int i = 0; i < stackTrace.Length; i++)
                    {
                        StackTraceElement element = stackTrace[i];
                        WriteString(element.ClassName, dataOut, bs);
                        WriteString(element.MethodName, bs);
                        WriteString(element.FileName, bs);
                        WriteInt(element.LineNumber, dataOut);
                    }
                    MarshalBrokerError(wireFormat, o.Cause, dataOut, bs);
                }
            }
        }
        
        /// <summary>
        /// Marshals the primitive type map to a byte array
        /// </summary>
        public static byte[] MarshalPrimitiveMap(IDictionary map)
        {
            if (map == null)
            {
                return null;
            }
            else
            {
                MemoryStream memoryStream = new MemoryStream();
                MarshalPrimitiveMap(map, new BinaryWriter(memoryStream));
                return memoryStream.GetBuffer();
            }
        }
        public static void MarshalPrimitiveMap(IDictionary map, BinaryWriter dataOut)
        {
            if (map == null)
            {
                WriteInt(-1, dataOut);
            }
            else
            {
                WriteInt(map.Count, dataOut);
                foreach (DictionaryEntry entry in map)
                {
                    String name = (String) entry.Key;
                    WriteUTF8(name, dataOut);
                    Object value = entry.Value;
                    MarshalPrimitive(dataOut, value);
                }
            }}
        
        
        
        /// <summary>
        /// Unmarshals the primitive type map from the given byte array
        /// </summary>
        public static  IDictionary UnmarshalPrimitiveMap(byte[] data)
        {
            if (data == null)
            {
                return new Hashtable();
            }
            else
            {
                return UnmarshalPrimitiveMap(new BinaryReader(new MemoryStream(data)));
            }
        }
        
        public static  IDictionary UnmarshalPrimitiveMap(BinaryReader dataIn)
        {
            int size = ReadInt(dataIn);
            if (size < 0)
            {
                return null;
            }
            else
            {
                IDictionary answer = new Hashtable(size);
                for (int i=0; i < size; i++)
                {
                    String name = ReadUTF8(dataIn);
                    answer[name] = UnmarshalPrimitive(dataIn);
                }
                return answer;
            }
            
        }
        
        public static void MarshalPrimitive(BinaryWriter dataOut, Object value)
        {
            if (value == null)
            {
                WriteByte(NULL, dataOut);
            }
            else if (value is bool)
            {
                WriteByte(BOOLEAN_TYPE, dataOut);
                WriteBoolean((bool) value, dataOut);
            }
            else if (value is byte)
            {
                WriteByte(BYTE_TYPE, dataOut);
                WriteByte(((Byte)value), dataOut);
            }
            else if (value is char)
            {
                WriteByte(CHAR_TYPE, dataOut);
                WriteChar((char) value, dataOut);
            }
            else if (value is short)
            {
                WriteByte(SHORT_TYPE, dataOut);
                WriteShort((short) value, dataOut);
            }
            else if (value is int)
            {
                WriteByte(INTEGER_TYPE, dataOut);
                WriteInt((int) value, dataOut);
            }
            else if (value is long)
            {
                WriteByte(LONG_TYPE, dataOut);
                WriteLong((long) value, dataOut);
            }
            else if (value is float)
            {
                WriteByte(FLOAT_TYPE, dataOut);
                WriteFloat((float) value, dataOut);
            }
            else if (value is double)
            {
                WriteByte(DOUBLE_TYPE, dataOut);
                WriteDouble((double) value, dataOut);
            }
            else if (value is byte[])
            {
                byte[] data = (byte[]) value;
                WriteByte(BYTE_ARRAY_TYPE, dataOut);
                WriteInt(data.Length, dataOut);
                dataOut.Write(data);
            }
            else if (value is string)
            {
                WriteByte(STRING_TYPE, dataOut);
                WriteUTF8((string) value, dataOut);
            }
            else
            {
                throw new IOException("Object is not a primitive: " + value);
            }
        }
        
        public static Object UnmarshalPrimitive(BinaryReader dataIn)
        {
            Object value=null;
            switch (ReadByte(dataIn))
            {
                case BYTE_TYPE:
                    value = ReadByte(dataIn);
                    break;
                case BOOLEAN_TYPE:
                    value = ReadBoolean(dataIn);
                    break;
                case CHAR_TYPE:
                    value = ReadChar(dataIn);
                    break;
                case SHORT_TYPE:
                    value = ReadShort(dataIn);
                    break;
                case INTEGER_TYPE:
                    value = ReadInt(dataIn);
                    break;
                case LONG_TYPE:
                    value = ReadLong(dataIn);
                    break;
                case FLOAT_TYPE:
                    value = ReadFloat(dataIn);
                    break;
                case DOUBLE_TYPE:
                    value = ReadDouble(dataIn);
                    break;
                case BYTE_ARRAY_TYPE:
                    int size = ReadInt(dataIn);
                    byte[] data = new byte[size];
                    dataIn.Read(data, 0, size);
                    value = data;
                    break;
                case STRING_TYPE:
                    value = ReadUTF8(dataIn);
                    break;
            }
            return value;
        }
        
        private static Object ReadDouble(BinaryReader dataIn)
        {
            // TODO: Implement this method
            return dataIn.ReadDouble();
        }
        
        /// <summary>
        /// Method ReadFloat
        /// </summary>
        /// <param name="dataIn">A  BinaryReader</param>
        /// <returns>An Object</retutns>
        private static Object ReadFloat(BinaryReader dataIn)
        {
            // TODO: Implement this method
            return (float) dataIn.ReadDouble();
        }
        
        private static Object ReadBoolean(BinaryReader dataIn)
        {
            // TODO: Implement this method
            return dataIn.ReadBoolean();
        }
        
        private static void WriteDouble(double value, BinaryWriter dataOut)
        {
            // TODO: Implement this method
            dataOut.Write(value);
        }
        
        private static void WriteFloat(float value, BinaryWriter dataOut)
        {
            // TODO: Implement this method
            dataOut.Write(value);
        }
        
        private static void WriteBoolean(bool value, BinaryWriter dataOut)
        {
            // TODO: Implement this method
            dataOut.Write(value);
        }
        
        
        public static void WriteUTF8(String text, BinaryWriter dataOut)
        {
            if (text != null)
            {
                int strlen = text.Length;
                int utflen = 0;
                int c, count = 0;
                
                char[] charr = text.ToCharArray();
                
                for (int i = 0; i < strlen; i++)
                {
                    c = charr[i];
                    if ((c >= 0x0001) && (c <= 0x007F))
                    {
                        utflen++;
                    }
                    else if (c > 0x07FF)
                    {
                        utflen += 3;
                    }
                    else
                    {
                        utflen += 2;
                    }
                }
                
                WriteInt(utflen, dataOut);
                byte[] bytearr = new byte[utflen];
                /*
                 byte[] bytearr = new byte[utflen + 4];
                 bytearr[count++] = (byte) ((utflen >>> 24) & 0xFF);
                 bytearr[count++] = (byte) ((utflen >>> 16) & 0xFF);
                 bytearr[count++] = (byte) ((utflen >>> 8) & 0xFF);
                 bytearr[count++] = (byte) ((utflen >>> 0) & 0xFF);
                 */
                for (int i = 0; i < strlen; i++)
                {
                    c = charr[i];
                    if ((c >= 0x0001) && (c <= 0x007F))
                    {
                        bytearr[count++] = (byte) c;
                    }
                    else if (c > 0x07FF)
                    {
                        bytearr[count++] = (byte) (0xE0 | ((c >> 12) & 0x0F));
                        bytearr[count++] = (byte) (0x80 | ((c >> 6) & 0x3F));
                        bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
                    }
                    else
                    {
                        bytearr[count++] = (byte) (0xC0 | ((c >> 6) & 0x1F));
                        bytearr[count++] = (byte) (0x80 | ((c >> 0) & 0x3F));
                    }
                }
                dataOut.Write(bytearr);
                
            }
            else
            {
                WriteInt(-1, dataOut);
            }
        }
        
        public static String ReadUTF8(BinaryReader dataIn)
        {
            int utflen = ReadInt(dataIn);
            if (utflen > -1)
            {
                StringBuilder str = new StringBuilder(utflen);
                
                byte[] bytearr = new byte[utflen];
                int c, char2, char3;
                int count = 0;
                
                dataIn.Read(bytearr, 0, utflen);
                
                while (count < utflen)
                {
                    c = bytearr[count] & 0xff;
                    switch (c >> 4)
                    {
                        case 0:
                        case 1:
                        case 2:
                        case 3:
                        case 4:
                        case 5:
                        case 6:
                        case 7:
                            /* 0xxxxxxx */
                            count++;
                            str.Append((char) c);
                            break;
                        case 12:
                        case 13:
                            /* 110x xxxx 10xx xxxx */
                            count += 2;
                            if (count > utflen)
                            {
                                throw CreateDataFormatException();
                            }
                            char2 = bytearr[count - 1];
                            if ((char2 & 0xC0) != 0x80)
                            {
                                throw CreateDataFormatException();
                            }
                            str.Append((char) (((c & 0x1F) << 6) | (char2 & 0x3F)));
                            break;
                        case 14:
                            /* 1110 xxxx 10xx xxxx 10xx xxxx */
                            count += 3;
                            if (count > utflen)
                            {
                                throw CreateDataFormatException();
                            }
                            char2 = bytearr[count - 2];
                            char3 = bytearr[count - 1];
                            if (((char2 & 0xC0) != 0x80) || ((char3 & 0xC0) != 0x80))
                            {
                                throw CreateDataFormatException();
                            }
                            str.Append((char) (((c & 0x0F) << 12) | ((char2 & 0x3F) << 6) | ((char3 & 0x3F) << 0)));
                            break;
                        default :
                            /* 10xx xxxx, 1111 xxxx */
                            throw CreateDataFormatException();
                    }
                }
// The number of chars produced may be less than utflen
                return str.ToString();
            }
            else
            {
                return null;
            }
        }
        
        private static Exception CreateDataFormatException()
        {
            // TODO: implement a better exception
            return new Exception("Data format error!");
        }
        
        
        /// <summary>
        /// Converts the object to a String
        /// </summary>
        public static string ToString(MessageId id)
        {
            return ToString(id.ProducerId) + ":" + id.ProducerSequenceId;
        }
        /// <summary>
        /// Converts the object to a String
        /// </summary>
        public static string ToString(ProducerId id)
        {
            return id.ConnectionId + ":" + id.SessionId + ":" + id.Value;
        }
        
        
        /// <summary>
        /// Converts the given transaction ID into a String
        /// </summary>
        public static String ToString(TransactionId txnId)
        {
            if (txnId is LocalTransactionId)
            {
                LocalTransactionId ltxnId = (LocalTransactionId) txnId;
                return "" + ltxnId.Value;
            }
            else if (txnId is XATransactionId)
            {
                XATransactionId xaTxnId = (XATransactionId) txnId;
                return "XID:" + xaTxnId.FormatId + ":" + ToHexFromBytes(xaTxnId.GlobalTransactionId) + ":" + ToHexFromBytes(xaTxnId.BranchQualifier);
            }
            return null;
        }
        
        /// <summary>
        /// Creates the byte array into hexidecimal
        /// </summary>
        public static String ToHexFromBytes(byte[] data)
        {
            StringBuilder buffer = new StringBuilder(data.Length * 2);
            for (int i = 0; i < data.Length; i++)
            {
                buffer.Append(HEX_TABLE[0xFF & data[i]]);
            }
            return buffer.ToString();
        }
        
    }
}

