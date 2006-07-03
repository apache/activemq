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
#include "InitDirector.h"

#include <activemq/logger/LogWriter.h>
#include <activemq/transport/IOTransportFactory.h>
#include <activemq/transport/TcpTransportFactory.h>
#include <activemq/connector/stomp/StompConnectorFactory.h>

using namespace activemq;
using namespace activemq::support;

int InitDirector::refCount;

////////////////////////////////////////////////////////////////////////////////
InitDirector::InitDirector(void)
{
    if( refCount == 0 )
    {
        logger::LogWriter::getInstance();
        connector::stomp::StompConnectorFactory::getInstance();
        transport::TcpTransportFactory::getInstance();
        transport::IOTransportFactory::getInstance();
    }
    
    refCount++;
}

////////////////////////////////////////////////////////////////////////////////
InitDirector::~InitDirector(void)
{
    refCount--;
    
    if( refCount == 0 )
    {
    }
}
