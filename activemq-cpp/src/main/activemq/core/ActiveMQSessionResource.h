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
#ifndef _ACTIVEMQ_CORE_ACTIVEMQSESSIONRESOURCE_H_
#define _ACTIVEMQ_CORE_ACTIVEMQSESSIONRESOURCE_H_

#include <activemq/connector/ConnectorResource.h>

namespace activemq{
namespace core{

    class ActiveMQSessionResource
    {
    public:
    
        virtual ~ActiveMQSessionResource(void) {}
    
        /**
         * Retrieve the Connector resource that is associated with
         * this Session resource.
         * @return pointer to a Connector Resource, can be NULL
         */
        virtual connector::ConnectorResource* getConnectorResource(void) = 0;

    };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQSESSIONRESOURCE_H_*/
