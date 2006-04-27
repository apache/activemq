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
#ifndef Ppr_IServerSocket_hpp_
#define Ppr_IServerSocket_hpp_

#include "ppr/net/ISocket.hpp"
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
struct IServerSocket : Interface
{
    /** Bind and listen to given IP/dns and port.
        @param host IP address or host name.
        @param port TCP port between 1..655535
    */
    virtual void bind(const char* host, int port) throw (SocketException) = 0 ;

    /** Bind and listen to given IP/dns and port.
        @param host IP address or host name.
        @param port TCP port between 1..655535
        @param backlog Size of listen backlog.
    */
    virtual void bind(const char* host, int port, int backlog) throw (SocketException) = 0 ;

    /** Blocks until a client connects to the bound socket.
        @return new socket. Never returns NULL.
    */
    virtual p<ISocket> accept() throw (SocketException) = 0 ;

    /** Closes the server socket.
    */
    virtual void close() = 0 ;

    /** @return true of the server socket is bound.
    */ 
    virtual bool isBound() const = 0 ;
};

/* namespace */
    }
  }
}

#endif /*Ppr_IServerSocket_hpp_*/

