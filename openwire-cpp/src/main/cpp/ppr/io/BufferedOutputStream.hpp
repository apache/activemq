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
#ifndef Ppr_BufferedOutputStream_hpp_
#define Ppr_BufferedOutputStream_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <stdlib.h>
#include "ppr/IllegalArgumentException.hpp"
#include "ppr/io/IOutputStream.hpp"
#include "ppr/io/IOException.hpp"
#include "ppr/thread/SimpleMutex.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace ppr
  {
    namespace io
    {
      using namespace ifr ;
      using namespace apache::ppr ;
      using namespace apache::ppr::thread ;

/*
 * Buffers bytes to provide more efficient writing to an
 * output stream.
 */
class BufferedOutputStream : public IOutputStream
{
private:
    p<IOutputStream> ostream ;
    char*            buffer ;
    int              size, position, treshold ;

    // Default buffer size
    static const int DEFAULT_SIZE = 10240 ;

public:
    BufferedOutputStream(p<IOutputStream> ostream) ;
    BufferedOutputStream(p<IOutputStream> ostream, int size) ;

    virtual void close() throw(IOException) ;
    virtual void flush() throw(IOException) ;
    virtual int write(const char* buf, int offset, int length) throw(IOException) ;

private:
    void checkClosed() throw(IOException) ;
    void flush0() throw(IOException) ;
    bool isEOB() ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_BufferedOutputStream_hpp_*/
