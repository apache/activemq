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
#include "command/ActiveMQTempQueue.hpp"

using namespace apache::activemq::client::command;

/*
 * 
 */
ActiveMQTempQueue::ActiveMQTempQueue()
   : ActiveMQDestination()
{
}

ActiveMQTempQueue::ActiveMQTempQueue(const char* name)
   : ActiveMQDestination(name)
{
}

ActiveMQTempQueue::~ActiveMQTempQueue()
{
}

p<string> ActiveMQTempQueue::getQueueName()
{
    return this->getPhysicalName() ;
}

int ActiveMQTempQueue::getDestinationType()
{
    return ActiveMQDestination::ACTIVEMQ_QUEUE ;
}

p<ActiveMQDestination> ActiveMQTempQueue::createDestination(const char* name)
{
    p<ActiveMQTempQueue> tempQueue = new ActiveMQTempQueue(name) ;
    return tempQueue ;
}
