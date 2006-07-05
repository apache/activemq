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
#ifndef Ppr_SocketInputStream_hpp_
#define Ppr_SocketInputStream_hpp_

#include "ppr/io/IInputStream.hpp"
#include "ppr/net/ISocket.hpp"
#include "ppr/net/SocketException.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace ppr
  {
    namespace io
    {
      using namespace ifr ;
      using namespace apache::ppr::net ;

/*
 * SocketInputStream reads primitive C++ data types from a
 * socket stream. It currently uses PPR sockets for
 * platform independency.
 */
class SocketInputStream : public IInputStream
{
private:
    p<ISocket> socket ;

public:
    SocketInputStream(p<ISocket> socket) ;
    virtual ~SocketInputStream() ;

    virtual void close() throw(IOException) ;
    virtual int read(char* buffer, int offset, int length) throw(IOException) ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_SocketInputStream_hpp_*/
