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
#include "marshal/BrokerIdMarshaller.hpp"

using namespace apache::activemq::client::marshal;

/*
 *
 */
BrokerIdMarshaller::BrokerIdMarshaller()
{
    // no-op
}

BrokerIdMarshaller::~BrokerIdMarshaller()
{
    // no-op
}

p<ICommand> BrokerIdMarshaller::createCommand()
{
    return new BrokerId() ;
}

void BrokerIdMarshaller::buildCommand(p<ICommand> command, p<BinaryReader> reader)
{
    //AbstractCommandMarshaller::buildCommand(command, reader) ;

    p<BrokerId> brokerId = (p<BrokerId>&)command ;
    brokerId->setValue( reader->readString()->c_str() ) ;
}

void BrokerIdMarshaller::writeCommand(p<ICommand> command, p<BinaryWriter> writer)
{
    //AbstractCommandMarshaller::writeCommand(command, writer) ;

    p<BrokerId> brokerId = (p<BrokerId>&)command ;
    writer->writeString( brokerId->getValue() ) ;
}
