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

using OpenWire.Client.Commands;
using OpenWire.Client.Core;
using OpenWire.Client.IO;

namespace OpenWire.Client.Core
{
    /// <summary>
    /// Represents the wire format
    /// </summary>
    public class OpenWireFormat
    {
        static private char[] MAGIC = new char[] { 'A', 'c', 't', 'i', 'v', 'e', 'M', 'Q' };
        
        private BaseDataStreamMarshaller[] dataMarshallers;
        private const byte NULL_TYPE = 0;
        private WireFormatInfo wireFormatInfo = new WireFormatInfo();
        
        public OpenWireFormat()
        {
            // lets configure the wire format
            wireFormatInfo.Magic = CreateMagicBytes();
            wireFormatInfo.Version = 1;
            wireFormatInfo.StackTraceEnabled = true;
            wireFormatInfo.TcpNoDelayEnabled = true;
			wireFormatInfo.PrefixPacketSize = true;
			wireFormatInfo.TightEncodingEnabled = true;
            
            dataMarshallers = new BaseDataStreamMarshaller[256];
            MarshallerFactory factory = new MarshallerFactory();
            factory.configure(this);
        }
        
        public WireFormatInfo WireFormatInfo {
            get {
                return wireFormatInfo;
            }
        }
        
        public bool StackTraceEnabled {
            get {
                return wireFormatInfo.StackTraceEnabled;
            }
        }
        
        public void addMarshaller(BaseDataStreamMarshaller marshaller)
        {
            byte type = marshaller.GetDataStructureType();
            dataMarshallers[type & 0xFF] = marshaller;
        }
        
        public void Marshal(Object o, BinaryWriter ds)
        {
            int size = 1;
            if (o != null)
            {
                DataStructure c = (DataStructure) o;
                byte type = c.GetDataStructureType();
                BaseDataStreamMarshaller dsm = (BaseDataStreamMarshaller) dataMarshallers[type & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + type);
                
                BooleanStream bs = new BooleanStream();
                size += dsm.TightMarshal1(this, c, bs);
                size += bs.MarshalledSize();
                
                BaseDataStreamMarshaller.WriteInt(size, ds);
                BaseDataStreamMarshaller.WriteByte(type, ds);
                bs.Marshal(ds);
                dsm.TightMarshal2(this, c, ds, bs);
            }
            else
            {
                BaseDataStreamMarshaller.WriteInt(size, ds);
                BaseDataStreamMarshaller.WriteByte(NULL_TYPE, ds);
            }
        }
        
        public Object Unmarshal(BinaryReader dis)
        {
            // lets ignore the size of the packet
            BaseDataStreamMarshaller.ReadInt(dis);
            
            // first byte is the type of the packet
            byte dataType = BaseDataStreamMarshaller.ReadByte(dis);
            if (dataType != NULL_TYPE)
            {
                BaseDataStreamMarshaller dsm = (BaseDataStreamMarshaller) dataMarshallers[dataType & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + dataType);
                //Console.WriteLine("Parsing type: " + dataType + " with: " + dsm);
                Object data = dsm.CreateObject();
                BooleanStream bs = new BooleanStream();
                bs.Unmarshal(dis);
                dsm.TightUnmarshal(this, data, dis, bs);
                return data;
            }
            else
            {
                return null;
            }
        }
        
        public int TightMarshalNestedObject1(DataStructure o, BooleanStream bs)
        {
            bs.WriteBoolean(o != null);
            if (o == null)
                return 0;
            
            if (o.IsMarshallAware())
            {
                MarshallAware ma = (MarshallAware) o;
                byte[] sequence = ma.GetMarshalledForm(this);
                bs.WriteBoolean(sequence != null);
                if (sequence != null)
                {
                    return 1 + sequence.Length;
                }
            }
            
            byte type = o.GetDataStructureType();
            if (type == 0) {
                throw new IOException("No valid data structure type for: " + o + " of type: " + o.GetType());
            }
            BaseDataStreamMarshaller dsm = (BaseDataStreamMarshaller) dataMarshallers[type & 0xFF];
            if (dsm == null)
                throw new IOException("Unknown data type: " + type);
            //Console.WriteLine("Marshalling type: " + type + " with structure: " + o);
            return 1 + dsm.TightMarshal1(this, o, bs);
        }
        
        public void TightMarshalNestedObject2(DataStructure o, BinaryWriter ds, BooleanStream bs)
        {
            if (!bs.ReadBoolean())
                return ;
            
            byte type = o.GetDataStructureType();
            BaseDataStreamMarshaller.WriteByte(type, ds);
            
            if (o.IsMarshallAware() && bs.ReadBoolean())
            {
                MarshallAware ma = (MarshallAware) o;
                byte[] sequence = ma.GetMarshalledForm(this);
                ds.Write(sequence, 0, sequence.Length);
            }
            else
            {
                
                BaseDataStreamMarshaller dsm = (BaseDataStreamMarshaller) dataMarshallers[type & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + type);
                dsm.TightMarshal2(this, o, ds, bs);
            }
        }
        
        public DataStructure TightUnmarshalNestedObject(BinaryReader dis, BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                
                byte dataType = BaseDataStreamMarshaller.ReadByte(dis);
                BaseDataStreamMarshaller dsm = (BaseDataStreamMarshaller) dataMarshallers[dataType & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + dataType);
                DataStructure data = dsm.CreateObject();
                
                if (data.IsMarshallAware() && bs.ReadBoolean())
                {
                    BaseDataStreamMarshaller.ReadInt(dis);
                    BaseDataStreamMarshaller.ReadByte(dis);
                    
                    BooleanStream bs2 = new BooleanStream();
                    bs2.Unmarshal(dis);
                    dsm.TightUnmarshal(this, data, dis, bs2);
                    
                    // TODO: extract the sequence from the dis and associate it.
                    //                MarshallAware ma = (MarshallAware)data
                    //                ma.setCachedMarshalledForm(this, sequence);
                }
                else
                {
                    dsm.TightUnmarshal(this, data, dis, bs);
                }
                
                return data;
            }
            else
            {
                return null;
            }
        }
        
        /// <summary>
        /// Method CreateMagicBytes
        /// </summary>
        private byte[] CreateMagicBytes()
        {
            byte[] answer = new byte[MAGIC.Length];
            for (int i = 0; i < answer.Length; i++)
            {
                answer[i] = (byte) MAGIC[i];
            }
            return answer;
        }
    }
}
