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
#include "activemq/command/ActiveMQTempTopic.hpp"

using namespace apache::activemq::command;

/*
 * 
 */
ActiveMQTempTopic::ActiveMQTempTopic()
   : ActiveMQTempDestination()
{
}

/*
 * 
 */
ActiveMQTempTopic::ActiveMQTempTopic(const char* name)
   : ActiveMQTempDestination(name)
{
}

/*
 * 
 */
ActiveMQTempTopic::~ActiveMQTempTopic()
{
}

/*
 * 
 */
unsigned char ActiveMQTempTopic::getDataStructureType()
{
    return ActiveMQTempTopic::TYPE ; 
}

/*
 * 
 */
p<string> ActiveMQTempTopic::getTopicName()
{
    return this->getPhysicalName() ;
}

/*
 * 
 */
int ActiveMQTempTopic::getDestinationType()
{
    return ActiveMQDestination::ACTIVEMQ_TOPIC ;
}

/*
 * 
 */
p<ActiveMQDestination> ActiveMQTempTopic::createDestination(const char* name)
{
    p<ActiveMQTempTopic> tempTopic = new ActiveMQTempTopic(name) ;
    return tempTopic ;
}
