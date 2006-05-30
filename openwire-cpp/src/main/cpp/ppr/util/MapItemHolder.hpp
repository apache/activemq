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
#ifndef Ppr_MapItemHolder_hpp_
#define Ppr_MapItemHolder_hpp_

#include <string>
#include <map>
#include "ppr/ConversionException.hpp"
#include "ppr/util/ifr/array"
#include "ppr/util/ifr/p"

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

namespace apache
{
  namespace ppr
  {
    namespace util
    {
      using namespace apache::ppr;
      using namespace std;
      using namespace ifr;

/*
 * 
 */
class MapItemHolder
{
private:
    void* value ;
    int   type,
          flags ;

    const static int BIT_DESTRUCT             = 0x01000000;
    const static int BIT_RELEASE_P_REFCOUNTED = BIT_DESTRUCT | 0x02000000;
    //const static int BIT_RELEASE_P_VOID       = BIT_DESTRUCT | 0x04000000;

public:
    const static int UNDEFINED = 0x00000000 ;
    const static int BOOLEAN   = 0x00000001 ;
    const static int BYTE      = 0x00000002 ;
    const static int BYTEARRAY = 0x00000004 ;
    const static int DOUBLE    = 0x00000008 ;
    const static int FLOAT     = 0x00000010 ;
    const static int INTEGER   = 0x00000020 ;
    const static int LONG      = 0x00000040 ;
    const static int SHORT     = 0x00000080 ;
    const static int STRING    = 0x00000100 ;

public:
    MapItemHolder() ;
    MapItemHolder(const MapItemHolder& other) ;
    MapItemHolder& operator = (const MapItemHolder& other) ;
    MapItemHolder(bool value) ;
    MapItemHolder(char value) ;
    MapItemHolder(array<char> value) ;
    MapItemHolder(double value) ;
    MapItemHolder(float value) ;
    MapItemHolder(int value) ;
    MapItemHolder(long long value) ;
    MapItemHolder(short value) ;
    MapItemHolder(p<string> value) ;
    MapItemHolder(const char* value) ;
    ~MapItemHolder() ;

    int getType() const ;
    bool getBoolean() const throw (ConversionException) ;
    char getByte() const throw (ConversionException) ;
    array<char> getBytes() const throw (ConversionException) ;
    double getDouble() const throw (ConversionException) ;
    float getFloat() const throw (ConversionException) ;
    int getInt() const throw (ConversionException) ;
    long long getLong() const throw (ConversionException) ;
    short getShort() const throw (ConversionException) ;
    p<string> getString() const throw (ConversionException) ;
} ;

typedef map<string, MapItemHolder> PropertyMap ;

/* namespace */
    }
  }
}

#endif /*Ppr_MapItemHolder_hpp_*/
