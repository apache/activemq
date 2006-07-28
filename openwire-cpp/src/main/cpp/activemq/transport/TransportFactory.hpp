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
#ifndef ActiveMQ_TransportFactory_hpp_
#define ActiveMQ_TransportFactory_hpp_

// Turn off warning message for ignored exception specification
#ifdef _MSC_VER
#pragma warning( disable : 4290 )
#endif

#include <string>
#include "activemq/protocol/IProtocol.hpp"
#include "activemq/protocol/openwire/OpenWireProtocol.hpp"
#include "activemq/transport/ITransport.hpp"
#include "activemq/transport/ITransportFactory.hpp"
#include "activemq/transport/LoggingFilter.hpp"
#include "activemq/transport/MutexFilter.hpp"
#include "activemq/transport/CorrelatorFilter.hpp"
#include "activemq/transport/tcp/TcpTransport.hpp"
#include "ppr/IllegalArgumentException.hpp"
#include "ppr/net/ISocket.hpp"
#include "ppr/net/Socket.hpp"
#include "ppr/net/SocketException.hpp"
#include "ppr/net/ISocketFactory.hpp"
#include "ppr/net/SocketFactory.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace transport
    {
      using namespace ifr ;
      using namespace std;
      using namespace apache::activemq::protocol;
      using namespace apache::activemq::protocol::openwire;
      using namespace apache::activemq::transport::tcp;
      using namespace apache::ppr::net;

/*
 * An implementation of ITransport that uses sockets to communicate with
 * the broker.
 */
class TransportFactory : public ITransportFactory
{
private:
    p<ISocketFactory> socketFactory ;

public:
    TransportFactory() ;
    virtual ~TransportFactory() {}

	virtual p<ITransport> createTransport(p<Uri> location) throw (SocketException, IllegalArgumentException) ;

protected:
    virtual p<ISocket> connect(const char* host, int port) throw (SocketException) ;
} ;

/* namespace */
    }
  }
}

#endif /*ActiveMQ_TransportFactory_hpp_*/
