/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
using ActiveMQ.Commands;
using System;
using System.Collections;
using System.IO;
using System.Text;

namespace ActiveMQ.OpenWire

{
    /// <summary>
    /// A base class with useful implementation inheritence methods
    /// for creating marshallers of the OpenWire protocol
    /// </summary>
    public abstract class BaseDataStreamMarshaller
    {
        
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
        
        public virtual int TightMarshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs)
        {
            return 0;
        }
        public virtual void TightMarshal2(
            OpenWireFormat wireFormat,
            Object o,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
        }
        
        public virtual void TightUnmarshal(
            OpenWireFormat wireFormat,
            Object o,
            BinaryReader dataIn,
            BooleanStream bs)
        {
        }
        
        
        protected virtual DataStructure TightUnmarshalNestedObject(
            OpenWireFormat wireFormat,
            BinaryReader dataIn,
            BooleanStream bs)
        {
            return wireFormat.TightUnmarshalNestedObject(dataIn, bs);
        }
        
        protected virtual int TightMarshalNestedObject1(
            OpenWireFormat wireFormat,
            DataStructure o,
            BooleanStream bs)
        {
            return wireFormat.TightMarshalNestedObject1(o, bs);
        }
        
        protected virtual void TightMarshalNestedObject2(
            OpenWireFormat wireFormat,
            DataStructure o,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
            wireFormat.TightMarshalNestedObject2(o, dataOut, bs);
        }
        
        protected virtual DataStructure TightUnmarshalCachedObject(
            OpenWireFormat wireFormat,
            BinaryReader dataIn,
            BooleanStream bs)
        {
            /*
             if (wireFormat.isCacheEnabled()) {
             if (bs.ReadBoolean()) {
             short index = dataIndataIn.ReadInt16()Int16();
             DataStructure value = wireFormat.UnmarshalNestedObject(dataIn, bs);
             wireFormat.setInUnmarshallCache(index, value);
             return value;
             } else {
             short index = dataIn.ReadInt16();
             return wireFormat.getFromUnmarshallCache(index);
             }
             } else {
             return wireFormat.UnmarshalNestedObject(dataIn, bs);
             }
             */
            return wireFormat.TightUnmarshalNestedObject(dataIn, bs);
        }
        
        protected virtual int TightMarshalCachedObject1(
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
            return wireFormat.TightMarshalNestedObject1(o, bs);
        }
        
        protected virtual void TightMarshalCachedObject2(
            OpenWireFormat wireFormat,
            DataStructure o,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
            /*
             if (wireFormat.isCacheEnabled()) {
             Short index = wireFormat.getMarshallCacheIndex(o);
             if (bs.ReadBoolean()) {
             dataOut.Write(index.shortValue(), dataOut);
             wireFormat.Marshal2NestedObject(o, dataOut, bs);
             } else {
             dataOut.Write(index.shortValue(), dataOut);
             }
             } else {
             wireFormat.Marshal2NestedObject(o, dataOut, bs);
             }
             */
            wireFormat.TightMarshalNestedObject2(o, dataOut, bs);
        }
        
        
        
        protected virtual String TightUnmarshalString(BinaryReader dataIn, BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                if (bs.ReadBoolean())
                {
                    return ReadAsciiString(dataIn);
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
                
        protected virtual int TightMarshalString1(String value, BooleanStream bs)
        {
            bs.WriteBoolean(value != null);
            if (value != null)
            {
                int strlen = value.Length;
                
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
            }
            else
            {
                return 0;
            }
        }
        
        public static void TightMarshalString2(String value, BinaryWriter dataOut, BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                // If we verified it only holds ascii values
                if (bs.ReadBoolean())
                {
                    dataOut.Write((short) value.Length);
                    // now lets write the bytes
                    char[] chars = value.ToCharArray();
                    for (int i = 0; i < chars.Length; i++)
                    {
                        dataOut.Write((byte)(chars[i]&0xFF00>>8));
                    }
                }
                else
                {
                    dataOut.Write(value);
                }
            }
        }
                
        public virtual int TightMarshalLong1(OpenWireFormat wireFormat, long o, BooleanStream bs)
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
        
        public virtual void TightMarshalLong2(
            OpenWireFormat wireFormat,
            long o,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                if (bs.ReadBoolean())
                {
                    dataOut.Write(o);
                }
                else
                {
                    dataOut.Write((int)o);
                }
            }
            else
            {
                if (bs.ReadBoolean())
                {
                    dataOut.Write((short)o);
                }
            }
        }
        public virtual long TightUnmarshalLong(OpenWireFormat wireFormat, BinaryReader dataIn, BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                if (bs.ReadBoolean())
                {
                    return dataIn.ReadInt64(); // dataIn.ReadInt64();
                }
                else
                {
                    return dataIn.ReadInt32();
                }
            }
            else
            {
                if (bs.ReadBoolean())
                {
                    return dataIn.ReadInt16();
                }
                else
                {
                    return 0;
                }
            }
        }
        protected virtual int TightMarshalObjectArray1(
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
                    rc += TightMarshalNestedObject1(wireFormat, objects[i], bs);
                }
                return rc;
            }
            else
            {
                bs.WriteBoolean(false);
                return 0;
            }
        }
        
        protected virtual void TightMarshalObjectArray2(
            OpenWireFormat wireFormat,
            DataStructure[] objects,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                dataOut.Write((short) objects.Length);
                for (int i = 0; i < objects.Length; i++)
                {
                    TightMarshalNestedObject2(wireFormat, objects[i], dataOut, bs);
                }
            }
        }
        
        
        protected virtual BrokerError TightUnmarshalBrokerError(
            OpenWireFormat wireFormat,
            BinaryReader dataIn,
            BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                BrokerError answer = new BrokerError();
                
                answer.ExceptionClass = TightUnmarshalString(dataIn, bs);
                answer.Message = TightUnmarshalString(dataIn, bs);
                if (wireFormat.StackTraceEnabled)
                {
                    short length = dataIn.ReadInt16();
                    StackTraceElement[] stackTrace = new StackTraceElement[length];
                    for (int i = 0; i < stackTrace.Length; i++)
                    {
                        StackTraceElement element = new StackTraceElement();
                        element.ClassName = TightUnmarshalString(dataIn, bs);
                        element.MethodName = TightUnmarshalString(dataIn, bs);
                        element.FileName = TightUnmarshalString(dataIn, bs);
                        element.LineNumber = dataIn.ReadInt32();
                        stackTrace[i] = element;
                    }
                    answer.StackTraceElements = stackTrace;
                    answer.Cause = TightUnmarshalBrokerError(wireFormat, dataIn, bs);
                }
                return answer;
            }
            else
            {
                return null;
            }
        }
        
        protected int TightMarshalBrokerError1(OpenWireFormat wireFormat, BrokerError o, BooleanStream bs)
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
                rc += TightMarshalString1(o.ExceptionClass, bs);
                rc += TightMarshalString1(o.Message, bs);
                if (wireFormat.StackTraceEnabled)
                {
                    rc += 2;
                    StackTraceElement[] stackTrace = o.StackTraceElements;
                    for (int i = 0; i < stackTrace.Length; i++)
                    {
                        StackTraceElement element = stackTrace[i];
                        rc += TightMarshalString1(element.ClassName, bs);
                        rc += TightMarshalString1(element.MethodName, bs);
                        rc += TightMarshalString1(element.FileName, bs);
                        rc += 4;
                    }
                    rc += TightMarshalBrokerError1(wireFormat, o.Cause, bs);
                }
                
                return rc;
            }
        }
        
        protected void TightMarshalBrokerError2(
            OpenWireFormat wireFormat,
            BrokerError o,
            BinaryWriter dataOut,
            BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                TightMarshalString2(o.ExceptionClass, dataOut, bs);
                TightMarshalString2(o.Message, dataOut, bs);
                if (wireFormat.StackTraceEnabled)
                {
                    StackTraceElement[] stackTrace = o.StackTraceElements;
                    dataOut.Write((short) stackTrace.Length);
                    
                    for (int i = 0; i < stackTrace.Length; i++)
                    {
                        StackTraceElement element = stackTrace[i];
                        TightMarshalString2(element.ClassName, dataOut, bs);
                        TightMarshalString2(element.MethodName, dataOut, bs);
                        TightMarshalString2(element.FileName, dataOut, bs);
                        dataOut.Write(element.LineNumber);
                    }
                    TightMarshalBrokerError2(wireFormat, o.Cause, dataOut, bs);
                }
            }
        }

        
        public virtual void LooseMarshal(
            OpenWireFormat wireFormat,
            Object o,
            BinaryWriter dataOut)
        {
        }
        
        public virtual void LooseUnmarshal(
            OpenWireFormat wireFormat,
            Object o,
            BinaryReader dataIn)
        {
        }
        
        
        protected virtual DataStructure LooseUnmarshalNestedObject(
            OpenWireFormat wireFormat,
            BinaryReader dataIn)
        {
            return wireFormat.LooseUnmarshalNestedObject(dataIn);
        }
        
        protected virtual void LooseMarshalNestedObject(
            OpenWireFormat wireFormat,
            DataStructure o,
            BinaryWriter dataOut)
        {
            wireFormat.LooseMarshalNestedObject(o, dataOut);
        }
        
        protected virtual DataStructure LooseUnmarshalCachedObject(
            OpenWireFormat wireFormat,
            BinaryReader dataIn)
        {
            /*
             if (wireFormat.isCacheEnabled()) {
             if (bs.ReadBoolean()) {
             short index = dataIndataIn.ReadInt16()Int16();
             DataStructure value = wireFormat.UnmarshalNestedObject(dataIn, bs);
             wireFormat.setInUnmarshallCache(index, value);
             return value;
             } else {
             short index = dataIn.ReadInt16();
             return wireFormat.getFromUnmarshallCache(index);
             }
             } else {
             return wireFormat.UnmarshalNestedObject(dataIn, bs);
             }
             */
            return wireFormat.LooseUnmarshalNestedObject(dataIn);
        }
        
        
        protected virtual void LooseMarshalCachedObject(
            OpenWireFormat wireFormat,
            DataStructure o,
            BinaryWriter dataOut)
        {
            /*
             if (wireFormat.isCacheEnabled()) {
             Short index = wireFormat.getMarshallCacheIndex(o);
             if (bs.ReadBoolean()) {
             dataOut.Write(index.shortValue(), dataOut);
             wireFormat.Marshal2NestedObject(o, dataOut, bs);
             } else {
             dataOut.Write(index.shortValue(), dataOut);
             }
             } else {
             wireFormat.Marshal2NestedObject(o, dataOut, bs);
             }
             */
            wireFormat.LooseMarshalNestedObject(o, dataOut);
        }
        
        
        
        protected virtual String LooseUnmarshalString(BinaryReader dataIn)
        {
            if (dataIn.ReadBoolean())
            {
                return dataIn.ReadString();
            }
            else
            {
                return null;
            }
        }
        
        
        public static void LooseMarshalString(String value, BinaryWriter dataOut)
        {
            dataOut.Write(value != null);
            if (value != null)
            {
                dataOut.Write(value);
            }
        }
                
        public virtual void LooseMarshalLong(
            OpenWireFormat wireFormat,
            long o,
            BinaryWriter dataOut)
        {
            dataOut.Write(o);
        }
        
        public virtual long LooseUnmarshalLong(OpenWireFormat wireFormat, BinaryReader dataIn)
        {
            return dataIn.ReadInt64();
        }
                
        protected virtual void LooseMarshalObjectArray(
            OpenWireFormat wireFormat,
            DataStructure[] objects,
            BinaryWriter dataOut)
        {
            dataOut.Write(objects!=null);
            if (objects!=null)
            {
                dataOut.Write((short) objects.Length);
                for (int i = 0; i < objects.Length; i++)
                {
                    LooseMarshalNestedObject(wireFormat, objects[i], dataOut);
                }
            }
        }
                
        protected virtual BrokerError LooseUnmarshalBrokerError(
            OpenWireFormat wireFormat,
            BinaryReader dataIn)
        {
            if (dataIn.ReadBoolean())
            {
                BrokerError answer = new BrokerError();
                
                answer.ExceptionClass = LooseUnmarshalString(dataIn);
                answer.Message = LooseUnmarshalString(dataIn);
                if (wireFormat.StackTraceEnabled)
                {
                    short length = dataIn.ReadInt16();
                    StackTraceElement[] stackTrace = new StackTraceElement[length];
                    for (int i = 0; i < stackTrace.Length; i++)
                    {
                        StackTraceElement element = new StackTraceElement();
                        element.ClassName = LooseUnmarshalString(dataIn);
                        element.MethodName = LooseUnmarshalString(dataIn);
                        element.FileName = LooseUnmarshalString(dataIn);
                        element.LineNumber = dataIn.ReadInt32();
                        stackTrace[i] = element;
                    }
                    answer.StackTraceElements = stackTrace;
                    answer.Cause = LooseUnmarshalBrokerError(wireFormat, dataIn);
                }
                return answer;
            }
            else
            {
                return null;
            }
        }
                
        protected void LooseMarshalBrokerError(
            OpenWireFormat wireFormat,
            BrokerError o,
            BinaryWriter dataOut)
        {
            dataOut.Write(o!=null);
            if (o!=null)
            {
                LooseMarshalString(o.ExceptionClass, dataOut);
                LooseMarshalString(o.Message, dataOut);
                if (wireFormat.StackTraceEnabled)
                {
                    StackTraceElement[] stackTrace = o.StackTraceElements;
                    dataOut.Write((short) stackTrace.Length);
                    
                    for (int i = 0; i < stackTrace.Length; i++)
                    {
                        StackTraceElement element = stackTrace[i];
                        LooseMarshalString(element.ClassName, dataOut);
                        LooseMarshalString(element.MethodName, dataOut);
                        LooseMarshalString(element.FileName, dataOut);
                        dataOut.Write(element.LineNumber);
                    }
                    LooseMarshalBrokerError(wireFormat, o.Cause, dataOut);
                }
            }
        }
        
        protected virtual byte[] ReadBytes(BinaryReader dataIn, bool flag)
        {
            if (flag)
            {
                int size = dataIn.ReadInt32();
                return dataIn.ReadBytes(size);
            }
            else
            {
                return null;
            }
        }
        
        protected virtual byte[] ReadBytes(BinaryReader dataIn)
        {
            int size = dataIn.ReadInt32();
            return dataIn.ReadBytes(size);
        }
        
        protected virtual byte[] ReadBytes(BinaryReader dataIn, int size)
        {
            return dataIn.ReadBytes(size);
        }
        
        protected virtual void WriteBytes(byte[] command, BinaryWriter dataOut)
        {
            dataOut.Write(command.Length);
            dataOut.Write(command);
        }
        
        protected virtual String ReadAsciiString(BinaryReader dataIn)
        {
            int size = dataIn.ReadInt16();
            byte[] data = new byte[size];
            dataIn.Read(data, 0, size);
            char[] text = new char[size];
            for (int i = 0; i < size; i++)
            {
                text[i] = (char) data[i];
            }
            return new String(text);
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

