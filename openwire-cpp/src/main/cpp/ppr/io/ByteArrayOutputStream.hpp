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
#ifndef Ppr_ByteArrayOutputStream_hpp_
#define Ppr_ByteArrayOutputStream_hpp_

#include "ppr/io/IOutputStream.hpp"
#include "ppr/util/ifr/array"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace ppr
  {
    namespace io
    {
      using namespace ifr;

/*
 * ByteArrayOutputStream writes primitive C++ data types to a
 * in memory byte array.
 */
class ByteArrayOutputStream : public IOutputStream
{
private:
    array<char> body ;
    int         bodySize,
                bodyLength,
                offset ;

    const static int INITIAL_SIZE = 256 ;
    const static int EXPAND_SIZE  = 128 ;

public:
    ByteArrayOutputStream() ;
    ByteArrayOutputStream(array<char> buffer) ;
    virtual ~ByteArrayOutputStream() ;

    virtual array<char> toArray() ;

    virtual void close() throw(IOException) ;
    virtual void flush() throw(IOException) ;
    virtual int write(const char* buffer, int index, int length) throw(IOException) ;

protected:
    virtual void expandBody() ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_ByteArrayOutputStream_hpp_*/
