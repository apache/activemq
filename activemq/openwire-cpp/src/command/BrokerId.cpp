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
#include "command/BrokerId.hpp"

using namespace apache::activemq::client::command;

/*
 *
 */
BrokerId::BrokerId()
{
    value = new string() ;
}

BrokerId::~BrokerId()
{
    // no-op
}

int BrokerId::getCommandType()
{
    return BrokerId::TYPE ;
}

p<string> BrokerId::getValue()
{
    return value ;
}

void BrokerId::setValue(const char* brokerId)
{
    this->value->assign( brokerId ) ;
}
