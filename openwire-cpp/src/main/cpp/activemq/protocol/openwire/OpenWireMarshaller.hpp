/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef ActiveMQ_OpenWireMarshaller_hpp_
#define ActiveMQ_OpenWireMarshaller_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <string>
#include <map>
#include "activemq/IDataStructure.hpp"
#include "activemq/command/AbstractCommand.hpp"
#include "activemq/command/WireFormatInfo.hpp"
#include "activemq/protocol/IMarshaller.hpp"
#include "activemq/protocol/openwire/OpenWireProtocol.hpp"
#include "ppr/io/DataOutputStream.hpp"
#include "ppr/io/DataInputStream.hpp"
#include "ppr/io/IOException.hpp"
#include "ppr/util/MapItemHolder.hpp"
#include "ppr/util/ifr/array"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace protocol
    {
      namespace openwire
      {
        using namespace ifr ;
        using namespace apache::activemq;
        using namespace apache::activemq::command;
        using namespace apache::activemq::protocol;
        using namespace apache::ppr::io;
        using namespace apache::ppr::util;

/*
 * A helper class with marshalling methods for the OpenWire protocol.
 */
class OpenWireMarshaller : public IMarshaller
{
private:
    p<WireFormatInfo> formatInfo ;

public:
    // Primitive types
    static const unsigned char TYPE_NULL      = 0 ;
    static const unsigned char TYPE_BOOLEAN   = 1 ;
    static const unsigned char TYPE_BYTE      = 2 ;
    static const unsigned char TYPE_CHAR      = 3 ;
    static const unsigned char TYPE_SHORT     = 4 ;
    static const unsigned char TYPE_INTEGER   = 5 ;
    static const unsigned char TYPE_LONG      = 6 ;
    static const unsigned char TYPE_DOUBLE    = 7 ;
    static const unsigned char TYPE_FLOAT     = 8 ;
    static const unsigned char TYPE_STRING    = 9 ;
    static const unsigned char TYPE_BYTEARRAY = 10 ;

public:
    OpenWireMarshaller(p<WireFormatInfo> formatInfo) ;

    virtual int marshalBoolean(bool value, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalByte(char value, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalShort(short value, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalInt(int value, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalLong(long long value, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalFloat(float value, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalDouble(double value, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalString(p<string> value, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalObject(p<IDataStructure> object, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalObjectArray(array<IDataStructure> object, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalByteArray(array<char> value, int mode, p<IOutputStream> writer) throw(IOException) ;
    virtual int marshalMap(p<PropertyMap> value, int mode, p<IOutputStream> writer) throw(IOException) ;

    virtual bool unmarshalBoolean(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual char unmarshalByte(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual short unmarshalShort(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual int unmarshalInt(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual long long unmarshalLong(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual float unmarshalFloat(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual double unmarshalDouble(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual p<string> unmarshalString(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual p<IDataStructure> unmarshalObject(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual array<IDataStructure> unmarshalObjectArray(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual array<char> unmarshalByteArray(int mode, p<IInputStream> reader) throw(IOException) ;
    virtual p<PropertyMap> unmarshalMap(int mode, p<IInputStream> reader) throw(IOException) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*ActiveMQ_OpenWireMarshaller_hpp_*/
