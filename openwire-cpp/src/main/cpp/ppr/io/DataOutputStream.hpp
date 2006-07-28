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
#ifndef Ppr_DataOutputStream_hpp_
#define Ppr_DataOutputStream_hpp_

#include "ppr/io/IOutputStream.hpp"
#include "ppr/util/Endian.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace ppr
  {
    namespace io
    {
      using namespace apache::ppr::util; // htonx and ntohx functions.
      using namespace ifr;

/*
 * The DataOutputStream class writes primitive C++ data types to an
 * underlying output stream in a Java compatible way. Strings
 * are written as raw bytes, no character encoding is performed.
 *
 * All numeric data types are written in big endian (network byte
 * order) and if the platform is little endian they are converted
 * automatically.
 *
 * Should any error occur an IOException will be thrown.
 */
class DataOutputStream : public IOutputStream
{
public:
    virtual void close() throw(IOException) = 0 ;
    virtual void flush() throw(IOException) = 0 ;
    virtual int write(const char* buffer, int index, int count) throw(IOException) = 0 ;
    virtual void writeByte(char v) throw(IOException) ;
    virtual void writeBoolean(bool v) throw(IOException) ;
    virtual void writeDouble(double v) throw(IOException) ;
    virtual void writeFloat(float v) throw(IOException) ;
    virtual void writeShort(short v) throw(IOException) ;
    virtual void writeInt(int v) throw(IOException) ;
    virtual void writeLong(long long v) throw(IOException) ;
    virtual void writeString(p<string> v) throw(IOException) ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_DataOutputStream_hpp_*/
