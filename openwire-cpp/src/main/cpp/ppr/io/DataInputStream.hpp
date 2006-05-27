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
#ifndef Ppr_DataInputStream_hpp_
#define Ppr_DataInputStream_hpp_

#include "ppr/io/IInputStream.hpp"
#include "ppr/io/encoding/ICharsetEncoder.hpp"
#include "ppr/io/encoding/CharsetEncoderRegistry.hpp"
#include "ppr/io/encoding/CharsetEncodingException.hpp"
#include "ppr/util/Endian.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace ppr
  {
    namespace io
    {
      using namespace ifr;
      using namespace apache::ppr::util; //htonx and ntohx functions.
      using namespace apache::ppr::io::encoding;

/*
 * The DataInputStream class reads primitive C++ data types from an
 * underlying input stream in a Java compatible way. Strings are
 * read as raw bytes or decoded should encoding have been configured.
 *
 * All numeric data types are assumed to be available in big
 * endian (network byte order) and are converted automatically
 * to little endian if needed by the platform.
 *
 * Should any error occur an IOException will be thrown.
 */
class DataInputStream : public IInputStream
{
private:
    p<IInputStream>    istream ;
    p<ICharsetEncoder> encoder ;

public:
    DataInputStream(p<IInputStream> istream) ;
    DataInputStream(p<IInputStream> istream, const char* encname) ;
    virtual ~DataInputStream() ;

    virtual void close() throw(IOException) ;
    virtual int read(char* buffer, int offset, int length) throw(IOException) ;
    virtual char readByte() throw(IOException) ;
    virtual bool readBoolean() throw(IOException) ;
    virtual double readDouble() throw(IOException) ;
    virtual float readFloat() throw(IOException) ;
    virtual short readShort() throw(IOException) ;
    virtual int readInt() throw(IOException) ;
    virtual long long readLong() throw(IOException) ;
    virtual p<string> readString() throw(IOException) ;

protected:
    void checkClosed() throw(IOException) ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_DataInputStream_hpp_*/
