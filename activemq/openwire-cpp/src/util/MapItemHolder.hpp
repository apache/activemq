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
#ifndef MapItemHolder_hpp_
#define MapItemHolder_hpp_

#include <string>
#include "command/ActiveMQDestination.hpp"
#include "util/ConversionException.hpp"
#include "util/ifr/ap.hpp"
#include "util/ifr/p.hpp"

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace util
      {
        using namespace ifr::v1;

/*
 * 
 */
class MapItemHolder
{
private:
    void* value ;
    int   type ;

    const static int BOOLEAN   = 0x00 ;
    const static int BYTE      = 0x01 ;
    const static int BYTEARRAY = 0x02 ;
    const static int DOUBLE    = 0x03 ;
    const static int FLOAT     = 0x04 ;
    const static int INTEGER   = 0x05 ;
    const static int LONG      = 0x06 ;
    const static int SHORT     = 0x07 ;
    const static int STRING    = 0x08 ;

public:
    MapItemHolder(bool value) ;
    MapItemHolder(char value) ;
    MapItemHolder(ap<char> value) ;
    MapItemHolder(double value) ;
    MapItemHolder(float value) ;
    MapItemHolder(int value) ;
    MapItemHolder(long long value) ;
    MapItemHolder(short value) ;
    MapItemHolder(p<string> value) ;
    virtual ~MapItemHolder() ;

    bool getBoolean() throw (ConversionException) ;
    char getByte() throw (ConversionException) ;
    ap<char> getBytes() throw (ConversionException) ;
    double getDouble() throw (ConversionException) ;
    float getFloat() throw (ConversionException) ;
    int getInt() throw (ConversionException) ;
    long long getLong() throw (ConversionException) ;
    short getShort() throw (ConversionException) ;
    p<string> getString() throw (ConversionException) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*MapItemHolder_hpp_*/
