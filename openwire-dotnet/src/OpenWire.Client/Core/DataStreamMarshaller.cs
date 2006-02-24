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

using OpenWire.Client.Commands;
using OpenWire.Client.Core;
using OpenWire.Client.IO;

namespace OpenWire.Client.Core
{
    /// <summary>
    /// A base class with useful implementation inheritence methods
    /// for creating marshallers of the OpenWire protocol
    /// </summary>
    public abstract class DataStreamMarshaller
    {
        
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
                else
                {
                    return dataIn.ReadString();
                }
            }
            else
            {
                return null;
            }
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
                    for (int i = 0; i < chars.Length; i++) {
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
            dataOut.Write(SwitchEndian(value));
        }
        
        public static void WriteShort(short value, BinaryWriter dataOut)
        {
            dataOut.Write(SwitchEndian(value));
        }
        
        public static void WriteInt(int value, BinaryWriter dataOut)
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
            for (int i = 0; i < 8; i++) {
                long lowest = x & 0xff;
                x >>= 8;
                answer <<= 8;
                answer += lowest;
            }
            return answer;
        }
        
        public static void WriteLong(long value, BinaryWriter dataOut)
        {
            dataOut.Write(IPAddress.HostToNetworkOrder(value));
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
                String clazz = ReadString(dataIn, bs);
                String message = ReadString(dataIn, bs);
                
                BrokerError answer = new BrokerError();
                answer.ExceptionClass = clazz;
                answer.Message = message;
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
            }
        }
    }
}
