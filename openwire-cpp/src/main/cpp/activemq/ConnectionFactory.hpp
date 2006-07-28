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
#ifndef ActiveMQ_ConnectionFactory_hpp_
#define ActiveMQ_ConnectionFactory_hpp_

// Must be included before any STL includes
#include "ppr/util/Guid.hpp"

#include <string>
#include "cms/IConnection.hpp"
#include "cms/IConnectionFactory.hpp"
#include "activemq/ConnectionException.hpp"
#include "activemq/command/ConnectionInfo.hpp"
#include "activemq/command/ConnectionId.hpp"
#include "activemq/protocol/IProtocol.hpp"
#include "activemq/transport/ITransport.hpp"
#include "activemq/transport/ITransportFactory.hpp"
#include "activemq/transport/TransportFactory.hpp"
#include "ppr/net/Uri.hpp"
#include "ppr/util/ifr/p"

namespace apache
{
  namespace activemq
  {
      using namespace apache::activemq::command;
      using namespace apache::activemq::protocol;
      using namespace apache::activemq::transport;
      using namespace apache::ppr::net;
      using namespace ifr;

/*
 * 
 */
class ConnectionFactory : public IConnectionFactory
{
private:
    p<Uri>               brokerUri ;
    p<string>            username ;
    p<string>            password ;
    p<string>            clientId ;
    p<IProtocol>         protocol ;
    p<ITransportFactory> transportFactory ;

public:
    // Constructors
    ConnectionFactory() ;
    ConnectionFactory(p<Uri> uri) ;

    // Attribute methods
    virtual p<Uri> getBrokerUri() ;
    virtual void setBrokerUri(p<Uri> brokerUri) ;
    virtual p<string> getUsername() ;
    virtual void setUsername(const char* username) ;
    virtual p<string> getPassword() ;
    virtual void setPassword(const char* password) ;
    virtual p<string> getClientId() ;
    virtual void setClientId(const char* clientId) ;

    // Operation methods
    virtual p<IConnection> createConnection() throw (ConnectionException) ;
    virtual p<IConnection> createConnection(const char* username, const char* password) throw (ConnectionException) ;

protected:
    // Implementation methods
    virtual p<ConnectionInfo> createConnectionInfo(const char* username, const char* password) ;
    virtual p<ITransport> createTransport() throw (ConnectionException) ;
} ;

/* namespace */
  }
}

#endif /*ActiveMQ_ConnectionFactory_hpp_*/
