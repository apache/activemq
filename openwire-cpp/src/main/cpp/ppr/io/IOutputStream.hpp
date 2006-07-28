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
#ifndef Ppr_IOutputStream_hpp_
#define Ppr_IOutputStream_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <string>
#include "ppr/io/IOException.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace ppr
  {
    namespace io
    {
      using namespace std;
      using namespace ifr;

/*
 * The IOutputStream interface provides for converting data from any of the
 * C++ primitive types to a series of bytes and writing these bytes to
 * a binary stream. Strings are written as raw bytes, no character
 * encoding is performed. If a byte cannot be written for any reason,
 * an IOException is thrown. 
 */
struct IOutputStream : Interface
{
    virtual void close() throw(IOException) = 0 ;
    virtual void flush() throw(IOException) = 0 ;
    virtual int write(const char* buffer, int index, int count) throw(IOException) = 0 ;
    virtual void writeByte(char v) throw(IOException) = 0 ;
    virtual void writeBoolean(bool v) throw(IOException) = 0 ;
    virtual void writeDouble(double v) throw(IOException) = 0 ;
    virtual void writeFloat(float v) throw(IOException) = 0 ;
    virtual void writeShort(short v) throw(IOException) = 0 ;
    virtual void writeInt(int v) throw(IOException) = 0 ;
    virtual void writeLong(long long v) throw(IOException) = 0 ;
    virtual void writeString(p<string> v) throw(IOException) = 0 ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_IOutputStream_hpp_*/
