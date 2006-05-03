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
#ifndef Ppr_ISocket_hpp_
#define Ppr_ISocket_hpp_

#include "ppr/net/SocketException.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace ppr
  {
    namespace net
    {
      using namespace ifr;

/*
 *
 */
struct ISocket : Interface
{
    /**
    */
    virtual void connect(const char* host, int port) throw (SocketException) = 0 ;
    /**
    */
    virtual void setSoTimeout(int millisecs) throw (SocketException) = 0;
    /**
    */
    virtual int  getSoTimeout() const = 0;
    /**
    */
    virtual int  receive(char* buff, int size) throw (SocketException) = 0;
    /**
    */
    virtual int  send(const unsigned char* buff, int size) throw (SocketException) = 0;
    /**
    */
    virtual int  send(const char* buff, int size) throw (SocketException) = 0;
    /**
    */
    virtual void close() = 0;
    /**
    */
    virtual bool isConnected() const = 0;
};

/* namespace */
    }
  }
}

#endif /*Ppr_ISocket_hpp_*/

