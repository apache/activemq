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
#ifndef ActiveMQ_IMarshaller_hpp_
#define ActiveMQ_IMarshaller_hpp_

#include <string>
#include <map>
#include "activemq/IDataStructure.hpp"
#include "ppr/io/IOutputStream.hpp"
#include "ppr/io/IInputStream.hpp"
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
      using namespace ifr ;
      using namespace std ;
      using namespace apache::activemq;
      using namespace apache::ppr::io;
      using namespace apache::ppr::util;

/*
 * Represents a wire protocol marshaller.
 */
struct IMarshaller : Interface
{
    // Marshal modes
    const static int MARSHAL_SIZE  = 1 ;
    const static int MARSHAL_WRITE = 2 ;
    const static int MARSHAL_READ  = 3 ;

    virtual int marshalBoolean(bool value, int mode, p<IOutputStream> writer) throw(IOException) = 0 ;
    virtual int marshalByte(char value, int mode, p<IOutputStream> writer) throw(IOException) = 0  ;
    virtual int marshalShort(short value, int mode, p<IOutputStream> writer) throw(IOException) = 0  ;
    virtual int marshalInt(int value, int mode, p<IOutputStream> writer) throw(IOException) = 0 ;
    virtual int marshalLong(long long value, int mode, p<IOutputStream> writer) throw(IOException) = 0 ;
    virtual int marshalFloat(float value, int mode, p<IOutputStream> writer) throw(IOException) = 0 ;
    virtual int marshalDouble(double value, int mode, p<IOutputStream> writer) throw(IOException) = 0 ;
    virtual int marshalString(p<string> value, int mode, p<IOutputStream> writer) throw(IOException) = 0 ;
    virtual int marshalObject(p<IDataStructure> object, int mode, p<IOutputStream> writer) throw(IOException) = 0 ;
    virtual int marshalObjectArray(array<IDataStructure> object, int mode, p<IOutputStream> writer) throw(IOException) = 0 ;
    virtual int marshalByteArray(array<char> value, int mode, p<IOutputStream> writer) throw(IOException) = 0 ;
    virtual int marshalMap(p<PropertyMap> value, int mode, p<IOutputStream> writer) throw(IOException) = 0 ;

    virtual bool unmarshalBoolean(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual char unmarshalByte(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual short unmarshalShort(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual int unmarshalInt(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual long long unmarshalLong(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual float unmarshalFloat(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual double unmarshalDouble(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual p<string> unmarshalString(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual p<IDataStructure> unmarshalObject(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual array<IDataStructure> unmarshalObjectArray(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual array<char> unmarshalByteArray(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
    virtual p<PropertyMap> unmarshalMap(int mode, p<IInputStream> reader) throw(IOException) = 0 ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_IMarshaller_hpp_*/
