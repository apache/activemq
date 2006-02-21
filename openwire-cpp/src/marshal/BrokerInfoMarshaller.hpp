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
#ifndef BrokerInfoMarshaller_hpp_
#define BrokerInfoMarshaller_hpp_

#include <string>
#include "command/ICommand.hpp"
#include "command/BrokerInfo.hpp"
#include "io/BinaryReader.hpp"
#include "io/BinaryWriter.hpp"
#include "marshal/AbstractCommandMarshaller.hpp"
#include "util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace marshal
      {
        using namespace ifr ;
        using namespace apache::activemq::client::command;
        using namespace apache::activemq::client::io;

/*
 *
 */
class BrokerInfoMarshaller : public AbstractCommandMarshaller
{
public:
    BrokerInfoMarshaller() ;
    virtual ~BrokerInfoMarshaller() ;

    virtual p<ICommand> createCommand() ;
    virtual void buildCommand(p<ICommand> command, p<BinaryReader> reader) ;
    virtual void writeCommand(p<ICommand> command, p<BinaryWriter> writer) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*BrokerInfoMarshaller_hpp_*/