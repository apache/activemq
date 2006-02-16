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
#include "ConnectionFactory.hpp"
#include "Connection.hpp"

using namespace apache::activemq::client;

/*
 *
 */
ConnectionFactory::ConnectionFactory()
{
    host = new string() ;
    host->assign("localhost") ;
    port = 61616 ;
    username = new string() ;
    password = new string() ;
    clientId = new string() ;
}

/*
 *
 */
ConnectionFactory::ConnectionFactory(const char* host, int port)
{
    this->host = new string() ;
    this->host->assign(host) ;
    this->port = port ;
    username = new string() ;
    password = new string() ;
    clientId = new string() ;
}

/*
 *
 */
ConnectionFactory::~ConnectionFactory()
{
}


// --- Attribute methods --------------------------------------------

/*
 *
 */
p<string> ConnectionFactory::getHost()
{
    return host ;
}

/*
 *
 */
void ConnectionFactory::setHost(const char* host)
{
    this->host->assign(host) ;
}

/*
 *
 */
int ConnectionFactory::getPort()
{
    return port ;
}

/*
 *
 */
void ConnectionFactory::setPort(int port)
{
    port = port ;
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
    this->username->assign(username) ;
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
    this->password->assign(password) ;
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
    this->clientId->assign(clientId) ;
}


// --- Operation methods --------------------------------------------

/*
 *
 */
p<IConnection> ConnectionFactory::createConnection()
{
    return createConnection(username->c_str(), password->c_str()) ;
}

/*
 *
 */
p<IConnection> ConnectionFactory::createConnection(const char* username, const char* password)
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

    connectionId->setValue( createNewConnectionId()->c_str() ) ;
    connectionInfo->setConnectionId( connectionId ) ;
    connectionInfo->setUsername( username ) ;
    connectionInfo->setPassword( password ) ;

    return connectionInfo ;
}

/*
 *
 */
p<string> ConnectionFactory::createNewConnectionId()
{
    return Guid::getGuidString() ; 
}

/*
 *
 */
p<ITransport> ConnectionFactory::createTransport()
{
    p<ITransport> transport = new SocketTransport(host->c_str(), port) ;
    return transport ;
}
