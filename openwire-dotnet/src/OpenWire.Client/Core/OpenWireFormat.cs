using System;
using System.IO;

using OpenWire.Client.Commands;
using OpenWire.Client.Core;
using OpenWire.Client.IO;

namespace OpenWire.Client.Core {
        /// <summary>
        /// Represents the wire format
        /// </summary>
        public class OpenWireFormat {
                private DataStreamMarshaller[] dataMarshallers;
                private const byte NULL_TYPE = 0;


                public void addMarshaller(DataStreamMarshaller marshaller) 
                {
                        dataMarshallers[marshaller.GetDataStructureType()] = marshaller;
                }
                
                public void Marshal(Object o, BinaryWriter ds) {
                        int size = 1;
                        if (o != null) {
                                DataStructure c = (DataStructure) o;
                                byte type = c.GetDataStructureType();
                                DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
                                if (dsm == null)
                                        throw new IOException("Unknown data type: " + type);

                                BooleanStream bs = new BooleanStream();
                                size += dsm.Marshal1(this, c, bs);
                                size += bs.MarshalledSize();

                                ds.Write(size);
                                ds.Write(type);
                                bs.Marshal(ds);
                                dsm.Marshal2(this, c, ds, bs);
                        } else {
                                ds.Write(size);
                                ds.Write(NULL_TYPE);
                        }
                }

                public Object Unmarshal(BinaryReader dis) {
                        byte dataType = dis.ReadByte();
                        if (dataType != NULL_TYPE) {
                                DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[dataType & 0xFF];
                                if (dsm == null)
                                        throw new IOException("Unknown data type: " + dataType);
                                Object data = dsm.CreateObject();
                                BooleanStream bs = new BooleanStream();
                                bs.Unmarshal(dis);
                                dsm.Unmarshal(this, data, dis, bs);
                                return data;
                        } else {
                                return null;
                        }
                }

                public int Marshal1NestedObject(DataStructure o, BooleanStream bs) {
                        bs.WriteBoolean(o != null);
                        if (o == null)
                                return 0;

                        if (o.IsMarshallAware()) {
                                MarshallAware ma = (MarshallAware) o;
                                byte[] sequence = ma.GetMarshalledForm(this);
                                bs.WriteBoolean(sequence != null);
                                if (sequence != null) {
                                        return 1 + sequence.Length;
                                }
                        }

                        byte type = o.GetDataStructureType();
                        DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
                        if (dsm == null)
                                throw new IOException("Unknown data type: " + type);
                        return 1 + dsm.Marshal1(this, o, bs);
                }

                public void Marshal2NestedObject(DataStructure o, BinaryWriter ds, BooleanStream bs) {
                        if (!bs.ReadBoolean())
                                return ;

                        byte type = o.GetDataStructureType();
                        ds.Write(type);

                        if (o.IsMarshallAware() && bs.ReadBoolean()) {
                                MarshallAware ma = (MarshallAware) o;
                                byte[] sequence = ma.GetMarshalledForm(this);
                                ds.Write(sequence, 0, sequence.Length);
                        } else {

                                DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[type & 0xFF];
                                if (dsm == null)
                                        throw new IOException("Unknown data type: " + type);
                                dsm.Marshal2(this, o, ds, bs);
                        }
                }

                public DataStructure UnmarshalNestedObject(BinaryReader dis, BooleanStream bs) {
                        if (bs.ReadBoolean()) {

                                byte dataType = dis.ReadByte();
                                DataStreamMarshaller dsm = (DataStreamMarshaller) dataMarshallers[dataType & 0xFF];
                                if (dsm == null)
                                        throw new IOException("Unknown data type: " + dataType);
                                DataStructure data = dsm.CreateObject();

                                if (data.IsMarshallAware() && bs.ReadBoolean()) {

                                        dis.ReadInt32();
                                        dis.ReadByte();

                                        BooleanStream bs2 = new BooleanStream();
                                        bs2.Unmarshal(dis);
                                        dsm.Unmarshal(this, data, dis, bs2);

                                        // TODO: extract the sequence from the dis and associate it.
                                        //                MarshallAware ma = (MarshallAware)data
                                        //                ma.setCachedMarshalledForm(this, sequence);
                                } else {
                                        dsm.Unmarshal(this, data, dis, bs);
                                }

                                return data;
                        } else {
                                return null;
                        }
                }
        }
}
