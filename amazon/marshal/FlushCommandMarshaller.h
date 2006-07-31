/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#ifndef FlushCommandMarshaller_h_
#define FlushCommandMarshaller_h_

#include <string>
#include <memory>

#include "command/IDataStructure.h"

/* auto-generated! */
/* we could cut this down  - for now include all possible headers */
#include "command/BrokerId.h"
#include "command/ConnectionId.h"
#include "command/ConsumerId.h"
#include "command/ProducerId.h"
#include "command/SessionId.h"
#include "command/BaseCommand.h"

#include "marshal/BinaryReader.h"
#include "marshal/BinaryWriter.h"

#include "marshal/BaseCommandMarshaller.h"

#include "marshal/ProtocolFormat.h"

namespace ActiveMQ {
  namespace Marshalling {

    class FlushCommandMarshaller : public BaseCommandMarshaller
    {
    public:
        FlushCommandMarshaller();
        virtual ~FlushCommandMarshaller();

        virtual auto_ptr<ActiveMQ::Command::IDataStructure> createCommand();
        virtual char getDataStructureType();
        
        virtual void unmarshal(ProtocolFormat& wireFormat,
                     ActiveMQ::Command::IDataStructure& o,
                     ActiveMQ::IO::BinaryReader& dataIn,
                     ActiveMQ::IO::BooleanStream& bs);

        virtual size_t marshal1(ProtocolFormat& wireFormat, 
                             const ActiveMQ::Command::IDataStructure& o,
                             ActiveMQ::IO::BooleanStream& bs);

        virtual void marshal2(ProtocolFormat& wireFormat, 
                              const ActiveMQ::Command::IDataStructure& o,
                              ActiveMQ::IO::BinaryWriter& dataOut,
                              ActiveMQ::IO::BooleanStream& bs);
    };
  }
}
#endif /*FlushCommandMarshaller_h_*/
