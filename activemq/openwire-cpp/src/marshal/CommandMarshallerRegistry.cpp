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
#include "marshal/CommandMarshallerRegistry.hpp"
#include "marshal/BrokerInfoMarshaller.hpp"
#include "marshal/BrokerIdMarshaller.hpp"

using namespace apache::activemq::client::marshal;


// --- Static initialization ----------------------------------------

p<BrokerInfoMarshaller> CommandMarshallerRegistry::brokerInfo = new BrokerInfoMarshaller() ;
p<BrokerIdMarshaller>   CommandMarshallerRegistry::brokerId   = new BrokerIdMarshaller() ;


/*
 *
 */
CommandMarshallerRegistry::CommandMarshallerRegistry()
{
    // no-op
}

/*
 *
 */
CommandMarshallerRegistry::~CommandMarshallerRegistry()
{
    // no-op
}


// --- Attribute methods --------------------------------------------


// --- Operation methods --------------------------------------------

/*
 *
 */
p<ICommand> CommandMarshallerRegistry::readCommand(p<BinaryReader> reader)
{
    // TODO
    return NULL ;
}

/*
 *
 */
void CommandMarshallerRegistry::writeCommand(p<ICommand> command, p<BinaryWriter> writer)
{
    // TODO
}

p<BrokerInfoMarshaller> CommandMarshallerRegistry::getBrokerInfoMarshaller()
{
    return brokerInfo ;
}

p<BrokerIdMarshaller> CommandMarshallerRegistry::getBrokerIdMarshaller()
{
    return brokerId ;
}
