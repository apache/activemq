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
#ifndef Ppr_ServerSocket_hpp_
#define Ppr_ServerSocket_hpp_

#include "ppr/net/IServerSocket.hpp"
#include "ppr/net/Socket.hpp"

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
class ServerSocket : public IServerSocket
{
public:
    typedef Socket::SocketHandle SocketHandle;
private:
    SocketHandle socketHandle;
public:
    /** Constructor.
        Creates a non-bound server socket.
    */
    ServerSocket();

    /** Destructor.
        Releases socket handle if close() hasn't been called.
    */
    virtual ~ServerSocket();
public:
    /** Bind and listen to given IP/dns and port.
        @param host IP address or host name.
        @param port TCP port between 1..655535
    */
    virtual void bind (const char* host, int port) throw (SocketException);

    /** Bind and listen to given IP/dns and port.
        @param host IP address or host name.
        @param port TCP port between 1..655535
        @param backlog Size of listen backlog.
    */
    virtual void bind (const char* host, int port, int backlog) throw (SocketException);

    /** Blocks until a client connects to the bound socket.
        @return new socket. Never returns NULL.
    */
    virtual p<ISocket> accept () throw (SocketException);

    /** Closes the server socket.
    */
    virtual void close();

    /** @return true of the server socket is bound.
    */ 
    virtual bool isBound() const;
};

/* namespace */
    }
  }
}

#endif /*Ppr_ServerSocket_hpp_*/

