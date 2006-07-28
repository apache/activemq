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
#ifndef Ppr_IInputStream_hpp_
#define Ppr_IInputStream_hpp_

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
 * The IInputStream interface provides for reading bytes from a binary stream
 * and reconstructing from them data in any of the C++ primitive types.
 * Strings are read as raw bytes, no character decoding is performed. If
 * any byte cannot be read for any reason, an IOException is thrown.
 */
struct IInputStream : Interface
{
    virtual void close() throw(IOException) = 0 ;
    virtual int read(char* buffer, int index, int count) throw(IOException) = 0 ;
    virtual char readByte() throw(IOException) = 0 ;
    virtual bool readBoolean() throw(IOException) = 0 ;
    virtual double readDouble() throw(IOException) = 0 ;
    virtual float readFloat() throw(IOException) = 0 ;
    virtual short readShort() throw(IOException) = 0 ;
    virtual int readInt() throw(IOException) = 0 ;
    virtual long long readLong() throw(IOException) = 0 ;
    virtual p<string> readString() throw(IOException) = 0 ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_IInputStream_hpp_*/
