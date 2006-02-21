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
#ifndef BinaryReader_hpp_
#define BinaryReader_hpp_

#include "util/Endian.hpp"
#include "io/IReader.hpp"
#include "util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
      namespace io
      {
        using namespace ifr;

/*
 * The BinaryReader class reads primitive C++ data types from an
 * underlying input stream in a Java compatible way. Strings are
 * read as raw bytes, no character decoding is performed.
 *
 * All numeric data types are assumed to be available in big
 * endian (network byte order) and are converted automatically
 * to little endian if needed by the platform.
 *
 * Should any error occur an IOException will be thrown.
 */
class BinaryReader : public IReader
{
public:
    BinaryReader() ;
    virtual ~BinaryReader() ;

    virtual void close() throw(IOException) = 0 ;
    virtual int read(char* buffer, int size)  throw(IOException) = 0 ;
    virtual char readByte() throw(IOException) ;
    virtual bool readBoolean() throw(IOException) ;
    virtual double readDouble() throw(IOException) ;
    virtual float readFloat() throw(IOException) ;
    virtual short readShort() throw(IOException) ;
    virtual int readInt() throw(IOException) ;
    virtual long long readLong() throw(IOException) ;
    virtual p<string> readString() throw(IOException) ;
} ;

/* namespace */
      }
    }
  }
}

#endif /*BinaryReader_hpp_*/
