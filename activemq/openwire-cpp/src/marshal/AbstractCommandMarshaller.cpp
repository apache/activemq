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
#include "marshal/AbstractCommandMarshaller.hpp"
#include "marshal/BrokerInfoMarshaller.hpp"
#include "marshal/BrokerIdMarshaller.hpp"


using namespace apache::activemq::client::marshal;

/*
 * 
 */
AbstractCommandMarshaller::AbstractCommandMarshaller()
{
}

AbstractCommandMarshaller::~AbstractCommandMarshaller()
{
}

p<ICommand> AbstractCommandMarshaller::readCommand(p<BinaryReader> reader)
{
    p<ICommand> command = createCommand() ;
    buildCommand(command, reader) ;
    return command ; 
}


// Implementation methods -----------------------------------------------------------

p<BrokerError> AbstractCommandMarshaller::readBrokerError(p<BinaryReader> reader)
{
    p<BrokerError> error = new BrokerError() ;
    error->setExceptionClass( reader->readString()->c_str() ) ;
    error->setStackTrace( reader->readString()->c_str() ) ;

    return error ;
}

void AbstractCommandMarshaller::writeBrokerError(p<BrokerError> command, p<BinaryWriter> writer)
{
     writer->writeString( command->getExceptionClass() ) ;
     writer->writeString( command->getStackTrace() ) ;
}

p<IDestination> AbstractCommandMarshaller::readDestination(p<BinaryReader> reader)
{
    return (p<IDestination>&)CommandMarshallerRegistry::readCommand(reader) ;
}

void AbstractCommandMarshaller::writeDestination(p<IDestination> command, p<BinaryWriter> writer)
{
    CommandMarshallerRegistry::writeCommand((p<ICommand>&)command, writer) ;
}

ap<BrokerId> AbstractCommandMarshaller::readBrokerIds(p<BinaryReader> reader)
{
    int size = reader->readInt() ;
    p<BrokerIdMarshaller> marshaller = CommandMarshallerRegistry::getBrokerIdMarshaller() ;

    ap<BrokerId> brokerIds (size);
    for( int i = 0; i < size; i++ )
        brokerIds[i] = (p<BrokerId>&)marshaller->readCommand(reader) ;

    return brokerIds ;
}

void AbstractCommandMarshaller::writeBrokerIds(ap<BrokerId> commands, p<BinaryWriter> writer)
{
    int size = (int)commands.size() ;
    p<BrokerIdMarshaller> marshaller = CommandMarshallerRegistry::getBrokerIdMarshaller() ;

    writer->writeInt(size) ;
    for( int i = 0; i < size; i++ )
        marshaller->writeCommand(commands[i], writer) ;
}

ap<BrokerInfo> AbstractCommandMarshaller::readBrokerInfos(p<BinaryReader> reader)
{
    int size = reader->readInt() ;
    p<BrokerInfoMarshaller> marshaller = CommandMarshallerRegistry::getBrokerInfoMarshaller() ;

    ap<BrokerInfo> brokerInfos (size) ;
    for( int i = 0; i < size; i++ )
        brokerInfos[i] = (p<BrokerInfo>&)marshaller->readCommand(reader) ;

    return brokerInfos ;
}

void AbstractCommandMarshaller::writeBrokerInfos(ap<BrokerInfo> commands, p<BinaryWriter> writer)
{
    int size = (int)commands.size() ; 
    p<BrokerInfoMarshaller> marshaller = CommandMarshallerRegistry::getBrokerInfoMarshaller() ;

    writer->writeInt(size) ;
    for( int i = 0; i < size; i++ )
        marshaller->writeCommand((p<ICommand>&)commands[i], writer); 
}

ap<IDataStructure> AbstractCommandMarshaller::readDataStructures(p<BinaryReader> reader)
{
    int size = reader->readInt() ;

    ap<IDataStructure> dataStructs (size) ;
    for( int i = 0; i < size; i++ )
        dataStructs[i] = (p<IDataStructure>&)CommandMarshallerRegistry::readCommand(reader) ;

    return dataStructs ;
}

void AbstractCommandMarshaller::writeDataStructures(ap<IDataStructure> commands, p<BinaryWriter> writer)
{
    int size = (int)commands.size() ; 

    writer->writeInt(size) ;
    for( int i = 0; i < size; i++ )
        CommandMarshallerRegistry::writeCommand((p<ICommand>&)commands[i], writer) ;
}

char* AbstractCommandMarshaller::readBytes(p<BinaryReader> reader)
{
    int size = reader->readInt() ;
    char* buffer = new char[size];
    reader->read(buffer, size) ;
    return buffer ;
}

void AbstractCommandMarshaller::writeBytes(char* buffer, int size, p<BinaryWriter> writer)
{
    writer->writeInt(size) ;
    writer->write(buffer, size) ;
}
