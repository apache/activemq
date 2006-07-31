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

#ifndef BaseDataStreamMarshaller_hpp_
#define BaseDataStreamMarshaller_hpp_

#include "command/IDataStructure.h"
#include "command/BrokerError.h"
#include "command/AbstractCommand.h"

#include "marshal/BinaryReader.h"
#include "marshal/BinaryWriter.h"
#include "marshal/BooleanStream.h"

#include <inttypes.h>

#include <exception>
#include <iostream>
#include <memory>
#include <boost/shared_ptr.hpp>

using namespace std;

namespace ActiveMQ {
  namespace Marshalling {
    class ProtocolFormat;
      
    class BaseDataStreamMarshaller
    {
    public:
        BaseDataStreamMarshaller() {};
        virtual ~BaseDataStreamMarshaller() {};

        virtual std::auto_ptr<ActiveMQ::Command::IDataStructure> createCommand() = 0;
        virtual char getDataStructureType() = 0;
    
        virtual void unmarshal(ProtocolFormat& wireFormat, ActiveMQ::Command::IDataStructure& o, ActiveMQ::IO::BinaryReader& dataIn, ActiveMQ::IO::BooleanStream& bs) = 0;
        virtual size_t marshal1(ProtocolFormat& wireFormat, const ActiveMQ::Command::IDataStructure& o, ActiveMQ::IO::BooleanStream& bs) = 0;
        virtual void   marshal2(ProtocolFormat& wireFormat, const ActiveMQ::Command::IDataStructure& o, ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs) = 0;

        static std::string unmarshalString(ProtocolFormat& wireFormat,
                                           ActiveMQ::IO::BinaryReader& dataIn,
                                           ActiveMQ::IO::BooleanStream& bs);
        
        static size_t writeString1(const std::string& str, ActiveMQ::IO::BooleanStream& bs);
        static void   writeString2(const std::string& str, ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs);

        static int64_t unmarshalLong(ProtocolFormat& wireFormat, ActiveMQ::IO::BinaryReader& dataIn, ActiveMQ::IO::BooleanStream& bs);
        static size_t writeLong1(int64_t i, ActiveMQ::IO::BooleanStream& bs);
        static void   writeLong2(int64_t i, ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs);

        static std::auto_ptr<ActiveMQ::Command::IDataStructure> unmarshalCachedObject(ProtocolFormat& wireFormat, ActiveMQ::IO::BinaryReader& dataIn, ActiveMQ::IO::BooleanStream& bs);
        static std::auto_ptr<ActiveMQ::Command::IDataStructure> unmarshalNestedObject(ProtocolFormat& wireFormat, ActiveMQ::IO::BinaryReader& dataIn, ActiveMQ::IO::BooleanStream& bs);
        static size_t marshal1BrokerError(ProtocolFormat& wireFormat, const boost::shared_ptr<const ActiveMQ::Command::BrokerError>& be, ActiveMQ::IO::BooleanStream& bs);
        static void   marshal2BrokerError(ProtocolFormat& wireFormat, const boost::shared_ptr<const ActiveMQ::Command::BrokerError>& be, ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs);
        static boost::shared_ptr<ActiveMQ::Command::BrokerError> unmarshalBrokerError(ProtocolFormat& wireFormat, ActiveMQ::IO::BinaryReader& dataIn, ActiveMQ::IO::BooleanStream& bs);

        static size_t marshal1CachedObject(ProtocolFormat& wireFormat,
                                           const ActiveMQ::Command::AbstractCommand& o,
                                           ActiveMQ::IO::BooleanStream& bs);
        
        static void   marshal2CachedObject(ProtocolFormat& wireFormat,
                                           const ActiveMQ::Command::AbstractCommand& o,
                                           ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs);
        
        static size_t marshal1NestedObject(ProtocolFormat& wireFormat,
                                           const ActiveMQ::Command::AbstractCommand& o,
                                           ActiveMQ::IO::BooleanStream& bs);
        
        static void   marshal2NestedObject(ProtocolFormat& wireFormat,
                                           const ActiveMQ::Command::AbstractCommand& o,
                                           ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs);
        
        static size_t marshal1CachedObject(ProtocolFormat& wireFormat,
                                           const ActiveMQ::Command::IDataStructure& o,
                                           ActiveMQ::IO::BooleanStream& bs);
        
        static void   marshal2CachedObject(ProtocolFormat& wireFormat,
                                           const ActiveMQ::Command::IDataStructure& o,
                                           ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs);
        
        static size_t marshal1NestedObject(ProtocolFormat& wireFormat,
                                           const ActiveMQ::Command::IDataStructure& o,
                                           ActiveMQ::IO::BooleanStream& bs);
        
        static void   marshal2NestedObject(ProtocolFormat& wireFormat,
                                           const ActiveMQ::Command::IDataStructure& o,
                                           ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs);
        
        static size_t marshal1CachedObject(ProtocolFormat& wireFormat,
                                           const boost::shared_ptr<const ActiveMQ::Command::IDataStructure>& o,
                                           ActiveMQ::IO::BooleanStream& bs);
        
        static void   marshal2CachedObject(ProtocolFormat& wireFormat,
                                           const boost::shared_ptr<const ActiveMQ::Command::IDataStructure>& o,
                                           ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs);
        
        static size_t marshal1NestedObject(ProtocolFormat& wireFormat,
                                           const boost::shared_ptr<const ActiveMQ::Command::IDataStructure>& o,
                                           ActiveMQ::IO::BooleanStream& bs);
        
        static void   marshal2NestedObject(ProtocolFormat& wireFormat,
                                           const boost::shared_ptr<const ActiveMQ::Command::IDataStructure>& o,
                                           ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs);
        
        template<class T> size_t marshal1ObjectArray(ProtocolFormat& wireFormat, const std::vector<boost::shared_ptr<T> >& arr, ActiveMQ::IO::BooleanStream& bs);
        template<class T> void   marshal2ObjectArray(ProtocolFormat& wireFormat, const std::vector<boost::shared_ptr<T> >& arr, ActiveMQ::IO::BinaryWriter& dataOut, ActiveMQ::IO::BooleanStream& bs);

        static std::vector<uint8_t> unmarshalConstByteSequence(ProtocolFormat& wireFormat,
                                                                     ActiveMQ::IO::BinaryReader& dataIn,
                                                                     ActiveMQ::IO::BooleanStream& bs,
                                                                     size_t num);

        static std::vector<uint8_t> unmarshalByteSequence(ProtocolFormat& wireFormat,
                                                                ActiveMQ::IO::BinaryReader& dataIn,
                                                                ActiveMQ::IO::BooleanStream& bs);
        
    };
  }
}

#endif /*BaseDataStreamMarshaller_hpp_*/
