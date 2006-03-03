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
#ifndef ConsumerInfoMarshaller_hpp_
#define ConsumerInfoMarshaller_hpp_

#include <string>

#include "command/DataStructure.hpp"

/* we could cut this down  - for now include all possible headers */
#include "command/BrokerId.hpp"
#include "command/ConnectionId.hpp"
#include "command/ConsumerId.hpp"
#include "command/ProducerId.hpp"
#include "command/SessionId.hpp"

#include "io/BinaryReader.hpp"
#include "io/BinaryWriter.hpp"

#include "command/BaseCommandMarshaller.hpp"
#include "util/ifr/p.hpp"

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
class ConsumerInfoMarshaller : public BaseCommandMarshaller
{
public:
    ConsumerInfoMarshaller() ;
    virtual ~ConsumerInfoMarshaller() ;

    virtual DataStructure* createCommand() ;
    virtual byte getDataStructureType() ;
    
    virtual void unmarshal(OpenWireFormat& wireFormat, Object o, BinaryReader& dataIn, BooleanStream& bs) ;
    virtual int marshal1(OpenWireFormat& wireFormat, Object& o, BooleanStream& bs) ;
    virtual void marshal2(OpenWireFormat& wireFormat, Object& o, BinaryWriter& dataOut, BooleanStream& bs) ;
} ;

/* namespace */
     }
    }
  }
}
#endif /*ConsumerInfoMarshaller_hpp_*/
