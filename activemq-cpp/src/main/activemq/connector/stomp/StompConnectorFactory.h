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
#ifndef ACTIVEMQ_CONNECTOR_STOMP_STOMPCONNECTORFACTORY_H_
#define ACTIVEMQ_CONNECTOR_STOMP_STOMPCONNECTORFACTORY_H_

#include <activemq/connector/ConnectorFactory.h>
#include <activemq/connector/ConnectorFactoryMapRegistrar.h>

namespace activemq{
namespace connector{
namespace stomp{

    class StompConnectorFactory : public connector::ConnectorFactory
    {
    public:
   
        virtual ~StompConnectorFactory(void) {}
   
        /** 
         * Creates a StompConnector
         * @param The Properties that the new connector is configured with
         */
        virtual Connector* createConnector(
            const activemq::util::Properties& properties,
            activemq::transport::Transport* transport );

        /**
         * Returns an instance of this Factory by reference
         * @return StompConnectorFactory reference
         */
        static ConnectorFactory& getInstance(void);

    };

}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_STOMPCONNECTORFACTORY_H_*/
