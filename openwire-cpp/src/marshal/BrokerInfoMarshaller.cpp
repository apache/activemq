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
#include "marshal/BrokerInfoMarshaller.hpp"

using namespace apache::activemq::client::marshal;

/*
 *
 */
BrokerInfoMarshaller::BrokerInfoMarshaller()
{
    // no-op
}

BrokerInfoMarshaller::~BrokerInfoMarshaller()
{
    // no-op
}

p<ICommand> BrokerInfoMarshaller::createCommand()
{
    return new BrokerInfo() ;
}

void BrokerInfoMarshaller::buildCommand(p<ICommand> command, p<BinaryReader> reader)
{
    //AbstractCommandMarshaller::buildCommand(command, reader) ;

    p<BrokerInfo> brokerInfo = (p<BrokerInfo>&)command ;
    brokerInfo->setBrokerId( (p<BrokerId>&)CommandMarshallerRegistry::getBrokerInfoMarshaller()->readCommand(reader) ) ;
    brokerInfo->setBrokerURL( reader->readString()->c_str() ) ;
    brokerInfo->setPeerBrokerInfo( AbstractCommandMarshaller::readBrokerInfos(reader) ) ;
    brokerInfo->setBrokerName( reader->readString()->c_str() ) ;
}

void BrokerInfoMarshaller::writeCommand(p<ICommand> command, p<BinaryWriter> writer)
{
    //AbstractCommandMarshaller::writeCommand(command, writer) ;

    p<BrokerInfo> brokerInfo = (p<BrokerInfo>&)command ;
    CommandMarshallerRegistry::getBrokerInfoMarshaller()->writeCommand(brokerInfo->getBrokerId(), writer) ;
    writer->writeString( brokerInfo->getBrokerURL() ) ;
    AbstractCommandMarshaller::writeBrokerInfos( brokerInfo->getPeerBrokerInfo(), writer) ;
    writer->writeString( brokerInfo->getBrokerName() ) ;
}
