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
#include "command/ConsumerId.hpp"

using namespace apache::activemq::client::command;

/*
 * Dummy, should be auto-generated
 */
ConsumerId::ConsumerId()
{
    connectionId = new string() ;
}

ConsumerId::~ConsumerId()
{
}

int ConsumerId::getCommandType()
{
    return ConsumerId::TYPE ;
}

void ConsumerId::setValue(long consumerId)
{
    this->consumerId = consumerId ;
}

long ConsumerId::getValue()
{
    return consumerId ;
}

void ConsumerId::setSessionId(long sessionId)
{
    this->sessionId = sessionId ;
}

long ConsumerId::getSessionId()
{
    return sessionId ;
}

void ConsumerId::setConnectionId(const char* connectionId)
{
    this->connectionId->assign( connectionId ) ;
}

p<string> ConsumerId::getConnectionId()
{
    return connectionId ;
}
