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
#ifndef CONNECTORFACTORY_H_
#define CONNECTORFACTORY_H_

#include <activemq/util/Properties.h>
#include <activemq/transport/Transport.h>
#include <activemq/connector/Connector.h>

namespace activemq{
namespace connector{

    /**
     * Interface class for all Connector Factory Classes
     */
    class ConnectorFactory
    {
    public:

        virtual ~ConnectorFactory(void) {};

        /** 
         * Creates a connector
         * @param The Properties that the new connector is configured with
         */
        virtual Connector* createConnector(
            const activemq::util::Properties& properties,
            activemq::transport::Transport*   transport) = 0;

   };

}}

#endif /*CONNECTORFACTORY_H_*/
