/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/

#include "BrokerError.h"

using namespace ActiveMQ::Command;

/*
 * 
 */
BrokerError::BrokerError()
{
}

BrokerError::~BrokerError()
{
}

const std::string& BrokerError::getExceptionClass()
{
    return exceptionClass_;
}

void BrokerError::setExceptionClass(const std::string& exceptionClass)
{
    exceptionClass_.assign(exceptionClass);
}

const std::string& BrokerError::getStackTrace()
{
    return stackTrace_;
}

void BrokerError::setStackTrace(const std::string& stackTrace)
{
    stackTrace_.assign(stackTrace);
}
