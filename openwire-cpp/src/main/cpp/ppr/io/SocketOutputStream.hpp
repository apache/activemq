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
#ifndef Ppr_SocketOutputStream_hpp_
#define Ppr_SocketOutputStream_hpp_

#include "ppr/io/DataOutputStream.hpp"
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
      using namespace apache::ppr::util ;

/*
 * SocketOutputStream writes primitive C++ data types to a
 * socket stream. It currently uses PPR sockets for
 * platform independency.
 */
class SocketOutputStream : public DataOutputStream
{
private:
    p<ISocket> socket ;

public:
    SocketOutputStream(p<ISocket> socket) ;
    virtual ~SocketOutputStream() ;

    virtual void close() throw(IOException) ;
    virtual void flush() throw(IOException) ;
    virtual int write(const char* buffer, int index, int size) throw(IOException) ;
} ;

/* namespace */
    }
  }
}

#endif /*Ppr_SocketOutputStream_hpp_*/
