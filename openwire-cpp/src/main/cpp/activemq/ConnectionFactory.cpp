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
#include "activemq/ConnectionFactory.hpp"
#include "activemq/Connection.hpp"
#include "activemq/protocol/openwire/OpenWireProtocol.hpp"
#include "activemq/transport/tcp/TcpTransport.hpp"

using namespace apache::activemq;

/*
 *
 */
ConnectionFactory::ConnectionFactory()
{
    // Use default URI
    brokerUri        = new Uri ("tcp://localhost:61616?wireFormat=openwire") ;
    username         = NULL ;
    password         = NULL ;
    clientId         = Guid::getGuidString() ;
    transportFactory = new TransportFactory() ;
}

/*
 *
 */
ConnectionFactory::ConnectionFactory(p<Uri> brokerUri)
{
    this->brokerUri  = brokerUri;
    username         = NULL ;
    password         = NULL ;
    clientId         = Guid::getGuidString() ;
    transportFactory = new TransportFactory() ;
}


// --- Attribute methods --------------------------------------------

/*
 *
 */
p<Uri> ConnectionFactory::getBrokerUri()
{
     return brokerUri ;
}

/*
 *
 */
void ConnectionFactory::setBrokerUri(p<Uri> brokerUri)
{
    this->brokerUri = brokerUri ;
}

/*
 *
 */
p<string> ConnectionFactory::getUsername()
{
    return username ;
}

/*
 *
 */
void ConnectionFactory::setUsername(const char* username)
{
    this->username = new string(username) ;
}

/*
 *
 */
p<string> ConnectionFactory::getPassword()
{
    return password ;
}

/*
 *
 */
void ConnectionFactory::setPassword(const char* password)
{
    this->password = new string(password);

}

/*
 *
 */
p<string> ConnectionFactory::getClientId()
{
    return clientId ;
}

/*
 *
 */
void ConnectionFactory::setClientId(const char* clientId)
{
    this->clientId = new string(clientId) ;
}


// --- Operation methods --------------------------------------------

/*
 *
 */
p<IConnection> ConnectionFactory::createConnection() throw (ConnectionException)
{
    return createConnection( (username != NULL) ? username->c_str() : NULL,
                             (password != NULL) ? password->c_str() : NULL ) ;
}

/*
 *
 */
p<IConnection> ConnectionFactory::createConnection(const char* username, const char* password) throw (ConnectionException)
{
    p<ConnectionInfo> connectionInfo ;
    p<ITransport>     transport ;
    p<Connection>     connection ;


    // Set up a new connection object
    connectionInfo = createConnectionInfo(username, password) ;
    transport      = createTransport() ;
    connection     = new Connection(transport, connectionInfo) ;
    connection->setClientId( clientId->c_str() ) ;

    return connection ;
}


// --- Implementation methods ---------------------------------------

/*
 *
 */
p<ConnectionInfo> ConnectionFactory::createConnectionInfo(const char* username, const char* password)
{
    p<ConnectionInfo> connectionInfo = new ConnectionInfo() ;
    p<ConnectionId>   connectionId   = new ConnectionId() ;
    p<string>         uid = (username != NULL) ? new string(username) : NULL ;
    p<string>         pwd = (password != NULL) ? new string(password) : NULL ;

    connectionId->setValue( Guid::getGuidString() ) ;
    connectionInfo->setConnectionId( connectionId ) ;
    connectionInfo->setUserName( uid ) ;
    connectionInfo->setPassword( pwd ) ;
    connectionInfo->setClientId( clientId ) ;

    return connectionInfo ;
}

/*
 *
 */
p<ITransport> ConnectionFactory::createTransport() throw (ConnectionException)
{
    try
    {
    	// Create a transport for given URI
        return transportFactory->createTransport( brokerUri ) ;
    }
    catch( SocketException se )
    {
        throw ConnectionException(__FILE__, __LINE__, "Failed to connect socket") ;
    }
}
