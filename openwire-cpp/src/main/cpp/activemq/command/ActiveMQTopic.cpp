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
#include "activemq/command/ActiveMQTopic.hpp"

using namespace apache::activemq::command;

/*
 * 
 */
ActiveMQTopic::ActiveMQTopic()
   : ActiveMQDestination()
{
}

/*
 * 
 */
ActiveMQTopic::ActiveMQTopic(const char* name)
   : ActiveMQDestination(name)
{
}

/*
 * 
 */
ActiveMQTopic::~ActiveMQTopic()
{
}

/*
 * 
 */
unsigned char ActiveMQTopic::getDataStructureType()
{
    return ActiveMQTopic::TYPE ; 
}

/*
 * 
 */
p<string> ActiveMQTopic::getTopicName()
{
    return this->getPhysicalName() ;
}

/*
 * 
 */
int ActiveMQTopic::getDestinationType()
{
    return ActiveMQDestination::ACTIVEMQ_TOPIC ;
}

/*
 * 
 */
p<ActiveMQDestination> ActiveMQTopic::createDestination(const char* name)
{
    p<ActiveMQTopic> topic = new ActiveMQTopic(name) ;
    return topic ;
}
