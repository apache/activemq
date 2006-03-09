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
using ActiveMQ.Commands;
using ActiveMQ.OpenWire.V1;
using System;
using System.IO;

namespace ActiveMQ.OpenWire
{
    /// <summary>
    /// Represents the wire format
    /// </summary>
    public class OpenWireFormat
    {
        
        private BaseDataStreamMarshaller[] dataMarshallers;
        private const byte NULL_TYPE = 0;
		
		private int version=1;
		private bool stackTraceEnabled=false;
		private bool tightEncodingEnabled=false;
		private bool sizePrefixDisabled=false;
		
        public OpenWireFormat()
        {
            dataMarshallers = new BaseDataStreamMarshaller[256];
            MarshallerFactory factory = new MarshallerFactory();
            factory.configure(this);
        }
                
        public bool StackTraceEnabled {
            get { return stackTraceEnabled; }
			set { stackTraceEnabled = value; }
        }
        public int Version {
            get { return version; }
			set { version = value; }
        }
        public bool SizePrefixDisabled {
            get { return sizePrefixDisabled; }
			set { sizePrefixDisabled = value; }
        }
        public bool TightEncodingEnabled {
            get { return tightEncodingEnabled; }
			set { tightEncodingEnabled = value; }
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
                BaseDataStreamMarshaller dsm = dataMarshallers[type & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + type);

                if(tightEncodingEnabled) {
					
					BooleanStream bs = new BooleanStream();
					size += dsm.TightMarshal1(this, c, bs);
					size += bs.MarshalledSize();
					
                    if( !sizePrefixDisabled ) {
						ds.Write(size);
					}
					
					ds.Write(type);
					bs.Marshal(ds);
					dsm.TightMarshal2(this, c, ds, bs);
					
				} else {
					
					BinaryWriter looseOut = ds;
					MemoryStream ms = null;
					// If we are prefixing then we need to first write it to memory,
					// otherwise we can write direct to the stream.
					if( !sizePrefixDisabled ) {
						ms= new MemoryStream();
						looseOut = new OpenWireBinaryWriter(ms);
						looseOut.Write(size);
					}
					
					looseOut.Write(type);
					dsm.LooseMarshal(this, c, looseOut);
					
					if( !sizePrefixDisabled ) {
						ms.Position=0;
						looseOut.Write( (int)ms.Length-4 );
						ds.Write(ms.GetBuffer(), 0, (int)ms.Length);
					}
				}
            }
            else
            {
                ds.Write(size);
                ds.Write(NULL_TYPE);
            }
        }
        
        public Object Unmarshal(BinaryReader dis)
        {
            // lets ignore the size of the packet
			if( !sizePrefixDisabled ) {
				dis.ReadInt32();
			}
            
            // first byte is the type of the packet
            byte dataType = dis.ReadByte();
            if (dataType != NULL_TYPE)
            {
                BaseDataStreamMarshaller dsm = dataMarshallers[dataType & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + dataType);
                //Console.WriteLine("Parsing type: " + dataType + " with: " + dsm);
                Object data = dsm.CreateObject();
				
				if(tightEncodingEnabled) {
					BooleanStream bs = new BooleanStream();
					bs.Unmarshal(dis);
					dsm.TightUnmarshal(this, data, dis, bs);
					return data;
				} else {
					dsm.LooseUnmarshal(this, data, dis);
					return data;
				}
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
            ds.Write(type);
            
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
                
                byte dataType = dis.ReadByte();
                BaseDataStreamMarshaller dsm = (BaseDataStreamMarshaller) dataMarshallers[dataType & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + dataType);
                DataStructure data = dsm.CreateObject();
                
                if (data.IsMarshallAware() && bs.ReadBoolean())
                {
                    dis.ReadInt32();
                    dis.ReadByte();
                    
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


        
        public void LooseMarshalNestedObject(DataStructure o, BinaryWriter dataOut)
        {
			dataOut.Write(o!=null);
			if( o!=null ) {
				byte type = o.GetDataStructureType();
				dataOut.Write(type);
                BaseDataStreamMarshaller dsm = (BaseDataStreamMarshaller) dataMarshallers[type & 0xFF];
				if( dsm == null )
					throw new IOException("Unknown data type: "+type);
				dsm.LooseMarshal(this, o, dataOut);
			}
        }
        
        public DataStructure LooseUnmarshalNestedObject(BinaryReader dis)
        {
            if (dis.ReadBoolean())
            {
                
                byte dataType = dis.ReadByte();
                BaseDataStreamMarshaller dsm = (BaseDataStreamMarshaller) dataMarshallers[dataType & 0xFF];
                if (dsm == null)
                    throw new IOException("Unknown data type: " + dataType);
                DataStructure data = dsm.CreateObject();
				dsm.LooseUnmarshal(this, data, dis);
                return data;
            }
            else
            {
                return null;
            }
        }
        
    }
}
