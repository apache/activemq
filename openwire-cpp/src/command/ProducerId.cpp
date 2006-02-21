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
#include "command/ProducerId.hpp"

using namespace apache::activemq::client::command;

/*
 * Dummy, should be auto-generated
 */
ProducerId::ProducerId()
{
    connectionId = new string() ;
}

ProducerId::~ProducerId()
{
}

int ProducerId::getCommandType()
{
    return ProducerId::TYPE ;
}

void ProducerId::setValue(long producerId)
{
    this->producerId = producerId ;
}

long ProducerId::getValue()
{
    return producerId ;
}

long ProducerId::getSessionId()
{
    return sessionId ;
}

void ProducerId::setSessionId(long sessionId)
{
    this->sessionId = sessionId ;
}

p<string> ProducerId::getConnectionId()
{
    return connectionId ;
}

void ProducerId::setConnectionId(const char* connectionId)
{
    this->connectionId->assign( connectionId ) ;
}
