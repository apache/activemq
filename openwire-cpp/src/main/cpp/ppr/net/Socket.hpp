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
#ifndef Ppr_Socket_hpp_
#define Ppr_Socket_hpp_

#include "ISocket.hpp"
#ifndef unix
#include <Winsock2.h> // SOCKET
#endif

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
class Socket : public ISocket
{
public:
#ifdef unix
    typedef int SocketHandle;
#else
    typedef SOCKET SocketHandle;
#endif
    static const SocketHandle INVALID_SOCKET_HANDLE = (SocketHandle) -1;
private:
    SocketHandle socketHandle;
public:
    /** Construct a non-connected socket.
    */
    Socket ();
    /** Construct a connected or bound socket based on given
        socket handle.
    */
    Socket (SocketHandle socketHandle);
    /** Destruct.
        Releases the socket handle but not
        gracefully shut down the connection.
    */
    virtual ~Socket();
public:
    /**
    */
    virtual void connect (const char* host, int port) throw (SocketException);
    /**
    */
    virtual void setSoTimeout (int millisec) throw (SocketException);
    /**
    */
    virtual int  getSoTimeout () const;
    /**
    */
    virtual int  receive (char* buff, int size) throw (SocketException);
    /**
    */
    virtual int  send (const unsigned char* buff, int size) throw (SocketException);
    /**
    */
    virtual int  send (const char* buff, int size) throw (SocketException);
    /**
    */
    virtual void close ();
    /**
    */
    virtual bool isConnected() const;
protected:
    SocketHandle getSocketHandle () {
        return socketHandle;
    }
    void setSocketHandle (SocketHandle socketHandle) {
        this->socketHandle = socketHandle;
    }
public:
#ifndef unix
    // WINDOWS needs initialization of winsock
    class StaticSocketInitializer {
    private:
        p<SocketException> socketInitError;
    public:
        p<SocketException> getSocketInitError () {
            return socketInitError;
        }
        StaticSocketInitializer ();
        virtual ~StaticSocketInitializer ();
    };
    static StaticSocketInitializer staticSocketInitializer;
#endif

};

/* namespace */
    }
  }
}

#endif /*Ppr_Socket_hpp_*/
