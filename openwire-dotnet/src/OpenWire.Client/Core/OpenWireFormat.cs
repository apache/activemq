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
        private DataStreamMarshaller[] dataMarshallers;
        private const byte NULL_TYPE = 0;
        
        
        public OpenWireFormat()
        {
            dataMarshallers = new DataStreamMarshaller[256];
            MarshallerFactory factory = new MarshallerFactory();
            factory.configure(this);
        }
        
        public void addMarshaller(DataStreamMarshaller marshaller)
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
                DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + type);
                
                BooleanStream bs = new BooleanStream();
                size += dsm.Marshal1(this, c, bs);
                size += bs.MarshalledSize();
                
                DataStreamMarshaller.WriteInt(size, ds);
                DataStreamMarshaller.WriteByte(type, ds);
                bs.Marshal(ds);
                dsm.Marshal2(this, c, ds, bs);
            }
            else
            {
                DataStreamMarshaller.WriteInt(size, ds);
                DataStreamMarshaller.WriteByte(NULL_TYPE, ds);
            }
        }
        
        public Object Unmarshal(BinaryReader dis)
        {
            int size = DataStreamMarshaller.ReadInt(dis);
            byte dataType = DataStreamMarshaller.ReadByte(dis);
            if (dataType != NULL_TYPE)
            {
                DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[dataType & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + dataType);
                //Console.WriteLine("Parsing type: " + dataType + " with: " + dsm);
                Object data = dsm.CreateObject();
                BooleanStream bs = new BooleanStream();
                bs.Unmarshal(dis);
                dsm.Unmarshal(this, data, dis, bs);
                return data;
            }
            else
            {
                return null;
            }
        }
        
        public int Marshal1NestedObject(DataStructure o, BooleanStream bs)
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
            DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
            if (dsm == null)
                throw new IOException("Unknown data type: " + type);
            //Console.WriteLine("Marshalling type: " + type + " with structure: " + o);
            return 1 + dsm.Marshal1(this, o, bs);
        }
        
        public void Marshal2NestedObject(DataStructure o, BinaryWriter ds, BooleanStream bs)
        {
            if (!bs.ReadBoolean())
                return ;
            
            byte type = o.GetDataStructureType();
            DataStreamMarshaller.WriteByte(type, ds);
            
            if (o.IsMarshallAware() && bs.ReadBoolean())
            {
                MarshallAware ma = (MarshallAware) o;
                byte[] sequence = ma.GetMarshalledForm(this);
                ds.Write(sequence, 0, sequence.Length);
            }
            else
            {
                
                DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + type);
                dsm.Marshal2(this, o, ds, bs);
            }
        }
        
        public DataStructure UnmarshalNestedObject(BinaryReader dis, BooleanStream bs)
        {
            if (bs.ReadBoolean())
            {
                
                byte dataType = DataStreamMarshaller.ReadByte(dis);
                DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[dataType & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + dataType);
                DataStructure data = dsm.CreateObject();
                
                if (data.IsMarshallAware() && bs.ReadBoolean())
                {
                    DataStreamMarshaller.ReadInt(dis);
                    DataStreamMarshaller.ReadByte(dis);
                    
                    BooleanStream bs2 = new BooleanStream();
                    bs2.Unmarshal(dis);
                    dsm.Unmarshal(this, data, dis, bs2);
                    
                    // TODO: extract the sequence from the dis and associate it.
                    //                MarshallAware ma = (MarshallAware)data
                    //                ma.setCachedMarshalledForm(this, sequence);
                }
                else
                {
                    dsm.Unmarshal(this, data, dis, bs);
                }
                
                return data;
            }
            else
            {
                return null;
            }
        }
    }
}
