using System;
using System.IO;

using OpenWire.Client.Commands;
using OpenWire.Client.Core;
using OpenWire.Client.IO;

namespace OpenWire.Client.Core {
        /// <summary>
        /// A base class with useful implementation inheritence methods
        /// for creating marshallers of the OpenWire protocol
        /// </summary>
        public abstract class DataStreamMarshaller {

                public abstract DataStructure CreateObject();
                public abstract byte GetDataStructureType();

                public virtual int Marshal1(OpenWireFormat wireFormat, Object o, BooleanStream bs) {
                        return 0;
                }
                public virtual void Marshal2(
                        OpenWireFormat wireFormat,
                        Object o,
                        BinaryWriter dataOut,
                        BooleanStream bs) {
                }

                public virtual void Unmarshal(
                        OpenWireFormat wireFormat,
                        Object o,
                        BinaryReader dataIn,
                        BooleanStream bs) {
                }

                public virtual int Marshal1Long(OpenWireFormat wireFormat, long o, BooleanStream bs) {
                        if (o == 0L) {
                                bs.WriteBoolean(false);
                                bs.WriteBoolean(false);
                                return 0;
                        } else {
                                ulong ul = (ulong) o;
                                if ((ul & 0xFFFFFFFFFFFF0000ul) == 0L) {
                                        bs.WriteBoolean(false);
                                        bs.WriteBoolean(true);
                                        return 2;
                                } else if ((ul & 0xFFFFFFFF00000000ul) == 0L) {
                                        bs.WriteBoolean(true);
                                        bs.WriteBoolean(false);
                                        return 4;
                                } else {
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
                        BooleanStream bs) {
                        if (bs.ReadBoolean()) {
                                if (bs.ReadBoolean()) {
                                        dataOut.Write(o);
                                } else {
                                        dataOut.Write((int) o);
                                }
                        } else {
                                if (bs.ReadBoolean()) {
                                        dataOut.Write((short) o);
                                }
                        }
                }
                public virtual long UnmarshalLong(OpenWireFormat wireFormat, BinaryReader dataIn, BooleanStream bs) {
                        if (bs.ReadBoolean()) {
                                if (bs.ReadBoolean()) {
                                        return dataIn.ReadInt64();
                                } else {
                                        return dataIn.ReadInt32();
                                }
                        } else {
                                if (bs.ReadBoolean()) {
                                        return dataIn.ReadInt16();
                                } else {
                                        return 0;
                                }
                        }
                }

                protected virtual DataStructure UnmarshalNestedObject(
                        OpenWireFormat wireFormat,
                        BinaryReader dataIn,
                        BooleanStream bs) {
                        return wireFormat.UnmarshalNestedObject(dataIn, bs);
                }

                protected virtual int Marshal1NestedObject(
                        OpenWireFormat wireFormat,
                        DataStructure o,
                        BooleanStream bs) {
                        return wireFormat.Marshal1NestedObject(o, bs);
                }

                protected virtual void Marshal2NestedObject(
                        OpenWireFormat wireFormat,
                        DataStructure o,
                        BinaryWriter dataOut,
                        BooleanStream bs) {
                        wireFormat.Marshal2NestedObject(o, dataOut, bs);
                }

                protected virtual DataStructure UnmarshalCachedObject(
                        OpenWireFormat wireFormat,
                        BinaryReader dataIn,
                        BooleanStream bs) {
                        /*
                        if (wireFormat.isCacheEnabled()) {
                                if (bs.ReadBoolean()) {
                                        short index = dataIn.ReadInt16();
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
                        return wireFormat.UnmarshalNestedObject(dataIn, bs);
                }

                protected virtual int Marshal1CachedObject(
                        OpenWireFormat wireFormat,
                        DataStructure o,
                        BooleanStream bs) {
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
                        BooleanStream bs) {
                        /*
                        if (wireFormat.isCacheEnabled()) {
                                Short index = wireFormat.getMarshallCacheIndex(o);
                                if (bs.ReadBoolean()) {
                                        dataOut.Write((short)index.shortValue());
                                        wireFormat.Marshal2NestedObject(o, dataOut, bs);
                                } else {
                                        dataOut.Write((short)index.shortValue());
                                }
                        } else {
                                wireFormat.Marshal2NestedObject(o, dataOut, bs);
                        }
                         */
                        wireFormat.Marshal2NestedObject(o, dataOut, bs);
                }



                protected virtual String ReadString(BinaryReader dataIn, BooleanStream bs) {
                        if (bs.ReadBoolean()) {
                                if (bs.ReadBoolean()) {
                                        int size = dataIn.ReadInt16();
                                        byte[] data = new byte[size];
                                        dataIn.Read(data, 0, size);
                                        char[] text = new char[size];
                                        for (int i = 0; i < size; i++) {
                                                text[i] = (char) data[i];
                                        }
                                        return new String(text);
                                } else {
                                        return dataIn.ReadString();
                                }
                        } else {
                                return null;
                        }
                }

                protected virtual int WriteString(String value, BooleanStream bs) {
                        bs.WriteBoolean(value != null);
                        if (value != null) {
                                int strlen = value.Length;
                                int utflen = 0;
                                int c = 0;
                                bool isOnlyAscii = true;
                                char[] charr = value.ToCharArray();
                                for (int i = 0; i < strlen; i++) {
                                        c = charr[i];
                                        if ((c >= 0x0001) && (c <= 0x007F)) {
                                                utflen++;
                                        } else if (c > 0x07FF) {
                                                utflen += 3;
                                                isOnlyAscii = false;
                                        } else {
                                                isOnlyAscii = false;
                                                utflen += 2;
                                        }
                                }

                                if (utflen >= Int16.MaxValue)
                                        throw new IOException("Encountered a String value that is too long to encode.");

                                bs.WriteBoolean(isOnlyAscii);
                                return utflen + 2;
                        } else {
                                return 0;
                        }
                }

                protected virtual void WriteString(String value, BinaryWriter dataOut, BooleanStream bs) {
                        if (bs.ReadBoolean()) {
                                // If we verified it only holds ascii values
                                if (bs.ReadBoolean()) {
                                        dataOut.Write((short) value.Length);
                                        dataOut.Write(value);
                                } else {
                                        dataOut.Write(value);
                                }
                        }
                }

                protected virtual int MarshalObjectArray(
                        OpenWireFormat wireFormat,
                        DataStructure[] objects,
                        BooleanStream bs) {
                        if (objects != null) {
                                int rc = 0;
                                bs.WriteBoolean(true);
                                rc += 2;
                                for (int i = 0; i < objects.Length; i++) {
                                        rc += Marshal1NestedObject(wireFormat, objects[i], bs);
                                }
                                return rc;
                        } else {
                                bs.WriteBoolean(false);
                                return 0;
                        }
                }

                protected virtual void MarshalObjectArray(
                        OpenWireFormat wireFormat,
                        DataStructure[] objects,
                        BinaryWriter dataOut,
                        BooleanStream bs) {
                        if (bs.ReadBoolean()) {
                                dataOut.Write((short) objects.Length);
                                for (int i = 0; i < objects.Length; i++) {
                                        Marshal2NestedObject(wireFormat, objects[i], dataOut, bs);
                                }
                        }
                }

                protected virtual byte[] ReadBytes(BinaryReader dataIn, bool flag) {
                        if (flag) {
                                int size = dataIn.ReadInt32();
                                return dataIn.ReadBytes(size);
                        } else {
                                return null;
                        }
                }

                protected virtual byte[] ReadBytes(BinaryReader dataIn) {
                        int size = dataIn.ReadInt32();
                        return dataIn.ReadBytes(size);
                }

                protected virtual byte[] ReadBytes(BinaryReader dataIn, int size) {
                        return dataIn.ReadBytes(size);
                }

                protected virtual void WriteBytes(byte[] command, BinaryWriter dataOut) {
                        dataOut.Write(command.Length);
                        dataOut.Write(command);
                }

                protected virtual BrokerError UnmarshalBrokerError(
                        OpenWireFormat wireFormat,
                        BinaryReader dataIn,
                        BooleanStream bs) {
                        if (bs.ReadBoolean()) {
                                String clazz = ReadString(dataIn, bs);
                                String message = ReadString(dataIn, bs);
                                
                                BrokerError answer = new BrokerError();
                                answer.ExceptionClass = clazz;
                                answer.Message = message;
                                return answer;
                        } else {
                                return null;
                        }
                }

                protected int MarshalBrokerError(OpenWireFormat wireFormat, BrokerError o, BooleanStream bs) {
                        if (o == null) {
                                bs.WriteBoolean(false);
                                return 0;
                        } else {
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
                        BooleanStream bs) {
                        if (bs.ReadBoolean()) {
                                WriteString(o.ExceptionClass, dataOut, bs);
                                WriteString(o.Message, dataOut, bs);
                        }
                }
                /*
                protected virtual ActiveMQDestination ReadDestination(BinaryReader dataIn) {
                        return (ActiveMQDestination) CommandMarshallerRegistry.ReadCommand(dataIn);
                }

                protected virtual void WriteDestination(ActiveMQDestination command, BinaryWriter dataOut) {
                        CommandMarshallerRegistry.WriteCommand(command, dataOut);
                }

                protected virtual BrokerId[] ReadBrokerIds(BinaryReader dataIn) {
                        int size = dataIn.ReadInt32();
                        BrokerId[] answer = new BrokerId[size];
                        for (int i = 0; i < size; i++) {
                                answer[i] = (BrokerId) CommandMarshallerRegistry.BrokerIdMarshaller.ReadCommand(dataIn);
                        }
                        return answer;
                }

                protected virtual void WriteBrokerIds(BrokerId[] commands, BinaryWriter dataOut) {
                        int size = commands.Length;
                        dataOut.Write(size);
                        for (int i = 0; i < size; i++) {
                                CommandMarshallerRegistry.BrokerIdMarshaller.WriteCommand(commands[i], dataOut);
                        }
                }


                protected virtual BrokerInfo[] ReadBrokerInfos(BinaryReader dataIn) {
                        int size = dataIn.ReadInt32();
                        BrokerInfo[] answer = new BrokerInfo[size];
                        for (int i = 0; i < size; i++) {
                                answer[i] = (BrokerInfo) CommandMarshallerRegistry
                                        .BrokerInfoMarshaller
                                        .ReadCommand(dataIn);
                        }
                        return answer;
                }

                protected virtual void WriteBrokerInfos(BrokerInfo[] commands, BinaryWriter dataOut) {
                        int size = commands.Length;
                        dataOut.Write(size);
                        for (int i = 0; i < size; i++) {
                                CommandMarshallerRegistry.BrokerInfoMarshaller.WriteCommand(commands[i], dataOut);
                        }
                }


                protected virtual DataStructure[] ReadDataStructures(BinaryReader dataIn) {
                        int size = dataIn.ReadInt32();
                        DataStructure[] answer = new DataStructure[size];
                        for (int i = 0; i < size; i++) {
                                answer[i] = (DataStructure) CommandMarshallerRegistry.ReadCommand(dataIn);
                        }
                        return answer;
                }

                protected virtual void WriteDataStructures(DataStructure[] commands, BinaryWriter dataOut) {
                        int size = commands.Length;
                        dataOut.Write(size);
                        for (int i = 0; i < size; i++) {
                                CommandMarshallerRegistry.WriteCommand((Command) commands[i], dataOut);
                        }
                }
                 */
        }
}
