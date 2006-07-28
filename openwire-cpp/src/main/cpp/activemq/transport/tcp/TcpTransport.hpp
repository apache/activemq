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
#ifndef ActiveMQ_TcpTransport_hpp_
#define ActiveMQ_TcpTransport_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <iostream>
#include <map>
#include "cms/CmsException.hpp"
#include "activemq/BrokerException.hpp"
#include "activemq/command/BaseCommand.hpp"
#include "activemq/command/Response.hpp"
#include "activemq/command/ExceptionResponse.hpp"
#include "activemq/protocol/IProtocol.hpp"
#include "activemq/transport/FutureResponse.hpp"
#include "activemq/transport/ITransport.hpp"
#include "activemq/transport/ICommandListener.hpp"
#include "ppr/InvalidOperationException.hpp"
#include "ppr/io/DataInputStream.hpp"
#include "ppr/io/DataOutputStream.hpp"
#include "ppr/io/BufferedInputStream.hpp"
#include "ppr/io/BufferedOutputStream.hpp"
#include "ppr/io/SocketInputStream.hpp"
#include "ppr/io/SocketOutputStream.hpp"
#include "ppr/net/ISocket.hpp"
#include "ppr/net/Socket.hpp"
#include "ppr/net/SocketException.hpp"
#include "ppr/net/ISocketFactory.hpp"
#include "ppr/net/SocketFactory.hpp"
#include "ppr/thread/SimpleMutex.hpp"
#include "ppr/thread/Thread.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace transport
    {
      namespace tcp
      {
        using namespace ifr ;
        using namespace std;
        using namespace apache::activemq;
        using namespace apache::activemq::command;
        using namespace apache::activemq::protocol;
        using namespace apache::ppr;
        using namespace apache::ppr::io;
        using namespace apache::ppr::net;
        using namespace apache::ppr::thread;
        using namespace apache::ppr::util;
        class ReadThread ;

/*
 * An implementation of ITransport that uses TCP to communicate with
 * the broker.
 */
class TcpTransport : public ITransport
{
private:
    p<IProtocol>        protocol ;
    p<DataInputStream>  istream ;
    p<DataOutputStream> ostream ;
    p<ICommandListener> listener ;
    p<ReadThread>       readThread ;
    p<ISocket>          socket ;
    bool                closed,
                        started ;

public:
    TcpTransport(p<ISocket> socket, p<IProtocol> wireProtocol) ;
    virtual ~TcpTransport() ;

    virtual void setCommandListener(p<ICommandListener> listener) ;
    virtual p<ICommandListener> getCommandListener() ;

    virtual void start() ;
    virtual void oneway(p<BaseCommand> command) ;
    virtual p<FutureResponse> asyncRequest(p<BaseCommand> command) ;
    virtual p<Response> request(p<BaseCommand> command) ;

public:
    void readLoop() ;
} ;

/*
 * 
 */
class ReadThread : public Thread
{
private:
    TcpTransport* transport ;

public:
    ReadThread(TcpTransport* transport)
    {
        this->transport = transport ;
    }

protected:
    virtual void run() throw(p<exception>)
    {
        transport->readLoop() ;
    }
} ;

/* namespace */
      }
    }
  }
}

#endif /*ActiveMQ_TcpTransport_hpp_*/
