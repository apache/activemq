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
 
#include <activemq/connector/stomp/StompConnectorFactory.h>
#include <activemq/connector/stomp/StompConnector.h>
#include <activemq/connector/Connector.h>
#include <activemq/transport/Transport.h>

using namespace activemq;
using namespace activemq::util;
using namespace activemq::transport;
using namespace activemq::connector;
using namespace activemq::connector::stomp;

////////////////////////////////////////////////////////////////////////////////

////////////////////////////////////////////////////////////////////////////////
Connector* StompConnectorFactory::createConnector(
    const activemq::util::Properties& properties,
    activemq::transport::Transport* transport )
{
    return dynamic_cast<Connector*>(
        new StompConnector( transport, properties ) );
}

////////////////////////////////////////////////////////////////////////////////
ConnectorFactory& StompConnectorFactory::getInstance(void)
{
    // Create a static instance of the registrar and return a reference to
    // its internal instance of this class.
    static ConnectorFactoryMapRegistrar registrar(
        "stomp", new StompConnectorFactory() );

    return registrar.getFactory();
}
