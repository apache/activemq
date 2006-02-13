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
#include "command/ConnectionInfo.hpp"

using namespace apache::activemq::client::command;

/*
 * Dummy, should be auto-generated
 */
ConnectionInfo::ConnectionInfo()
{
    connectionId = NULL ;
    brokerPath   = NULL ;
    username     = new string() ;
    password     = new string() ;
    clientId     = new string() ;
}

ConnectionInfo::~ConnectionInfo()
{
}

p<ConnectionId> ConnectionInfo::getConnectionId()
{
    return connectionId ;
}

void ConnectionInfo::setConnectionId(p<ConnectionId> connectionId)
{
    this->connectionId = connectionId ;
}

p<string> ConnectionInfo::getUsername()
{
    return this->username ;
}

void ConnectionInfo::setUsername(const char* username)
{
    this->username->assign( username ) ;
}

p<string> ConnectionInfo::getPassword()
{
    return this->password ;
}

void ConnectionInfo::setPassword(const char* password)
{
    this->password->assign( password ) ;
}

p<string> ConnectionInfo::getClientId()
{
    return this->clientId ;
}

void ConnectionInfo::setClientId(const char* clientId)
{
    this->clientId->assign( clientId ) ;
}

p<BrokerId*> ConnectionInfo::getBrokerPath()
{
    return this->brokerPath ;
}

void ConnectionInfo::setBrokerPath(p<BrokerId*> brokerPath)
{
    this->brokerPath = brokerPath ;
}
