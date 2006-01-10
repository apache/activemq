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
        public abstract class AbstractCommandMarshaller {

                public abstract Command CreateCommand();

                public virtual Command ReadCommand(BinaryReader dataIn) {
                        Command command = CreateCommand();
                        BuildCommand(command, dataIn);
                        return command; 
                }

                public virtual void BuildCommand(Command command, BinaryReader dataIn) {
                        // empty body to allow generated code to call base method
                }

                public virtual void WriteCommand(Command command, BinaryWriter dataOut) {
                        // empty body to allow generated code to call base method
                }

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


                protected virtual byte[] ReadBytes(BinaryReader dataIn) {
                        int size = dataIn.ReadInt32();
                        return dataIn.ReadBytes(size); 
                }

                protected virtual void WriteBytes(byte[] command, BinaryWriter dataOut) {
                        dataOut.Write(command.Length);
                        dataOut.Write(command); 
                } 
        } 
}
