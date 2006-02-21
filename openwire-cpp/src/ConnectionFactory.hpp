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
#ifndef ConnectionFactory_hpp_
#define ConnectionFactory_hpp_

// Must be included before any STL includes
#include "util/Guid.hpp"

#include <string>
#include "IConnection.hpp"
#include "IConnectionFactory.hpp"
#include "command/ConnectionInfo.hpp"
#include "command/ConnectionId.hpp"
#include "transport/ITransport.hpp"
#include "transport/SocketTransport.hpp"
#include "util/ifr/p"

namespace apache
{
  namespace activemq
  {
    namespace client
    {
        using namespace apache::activemq::client::command;
        using namespace apache::activemq::client::transport;
        using namespace apache::activemq::client::util;
        using namespace ifr;

/*
 * 
 */
class ConnectionFactory : IConnectionFactory
{
private:
    p<string> host,
              username,
              password,
              clientId ;
    int       port ;

public:
    // Constructors
    ConnectionFactory() ;
    ConnectionFactory(const char* host, int port) ;
    virtual ~ConnectionFactory() ;

    // Attribute methods
    virtual p<string> getHost() ;
    virtual void setHost(const char* host) ;
    virtual int getPort() ;
    virtual void setPort(int port) ;
    virtual p<string> getUsername() ;
    virtual void setUsername(const char* username) ;
    virtual p<string> getPassword() ;
    virtual void setPassword(const char* password) ;
    virtual p<string> getClientId() ;
    virtual void setClientId(const char* clientId) ;

    // Operation methods
    virtual p<IConnection> createConnection() ;
    virtual p<IConnection> createConnection(const char* username, const char* password) ;

protected:
    // Implementation methods
    virtual p<ConnectionInfo> createConnectionInfo(const char* username, const char* password) ;
    virtual p<string> createNewConnectionId() ;
    virtual p<ITransport> createTransport() ;
} ;

/* namespace */
    }
  }
}

#endif /*ConnectionFactory_hpp_*/