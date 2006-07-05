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
#ifndef Ppr_ByteArrayInputStream_hpp_
#define Ppr_ByteArrayInputStream_hpp_

#include "ppr/io/IInputStream.hpp"
#include "ppr/io/EOFException.hpp"
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
 * ByteArrayInputStream reads primitive C++ data types from an
 * in-memory byte array.
 */
class ByteArrayInputStream : public IInputStream
{
private:
    array<char> body ;
    int         bodySize,
                offset ;

public:
    ByteArrayInputStream(array<char> buffer) ;
    ~ByteArrayInputStream() ;

    virtual array<char> toArray() ;

    virtual void close() throw(IOException) ;
    virtual int read(char* buffer, int index, int count) throw(IOException) ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_ByteArrayInputStream_hpp_*/
