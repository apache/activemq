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
#ifndef AbstractCommandMarshaller_hpp_
#define AbstractCommandMarshaller_hpp_

#include "IDestination.hpp"
#include "BrokerError.hpp"
#include "command/ICommand.hpp"
#include "command/IDataStructure.hpp"
#include "command/BrokerInfo.hpp"
#include "command/BrokerId.hpp"
#include "io/BinaryReader.hpp"
#include "io/BinaryWriter.hpp"
#include "marshal/CommandMarshallerRegistry.hpp"
#include "util/ifr/p.hpp"
#include "util/ifr/ap.hpp"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace marshal
      {
        using namespace ifr ;
        using namespace std ;
        using namespace apache::activemq::client;
        using namespace apache::activemq::client::command;
        using namespace apache::activemq::client::io;

/*
 * 
 */
class AbstractCommandMarshaller
{
public:
    AbstractCommandMarshaller() ;
    virtual ~AbstractCommandMarshaller() ;

    virtual p<ICommand> createCommand() = 0 ;
    virtual p<ICommand> readCommand(p<BinaryReader> reader) ;
    virtual void buildCommand(p<ICommand> command, p<BinaryReader> reader) = 0 ;
    virtual void writeCommand(p<ICommand> command, p<BinaryWriter> writer) = 0 ;

protected:
    virtual p<BrokerError> readBrokerError(p<BinaryReader> reader) ;
    virtual void writeBrokerError(p<BrokerError> command, p<BinaryWriter> writer) ;
    virtual p<IDestination> readDestination(p<BinaryReader> reader) ;
    virtual void writeDestination(p<IDestination> command, p<BinaryWriter> writer) ;
    virtual ap<BrokerId> readBrokerIds(p<BinaryReader> reader) ;
    virtual void writeBrokerIds(ap<BrokerId> commands, p<BinaryWriter> writer) ;
    virtual ap<BrokerInfo> readBrokerInfos(p<BinaryReader> reader) ;
    virtual void writeBrokerInfos(ap<BrokerInfo> commands, p<BinaryWriter> writer) ;
    virtual ap<IDataStructure> readDataStructures(p<BinaryReader> reader) ;
    virtual void writeDataStructures(ap<IDataStructure> commands, p<BinaryWriter> writer) ;
    virtual char* readBytes(p<BinaryReader> reader) ;
    virtual void writeBytes(char* buffer, int size, p<BinaryWriter> writer) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*AbstractCommandMarshaller_hpp_*/
