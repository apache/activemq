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

#ifndef _ACTIVEMQ_TRANSPORT_TCPTRANSPORTFACTORY_H_
#define _ACTIVEMQ_TRANSPORT_TCPTRANSPORTFACTORY_H_

#include <activemq/transport/TransportFactory.h>
#include <activemq/transport/TransportFactoryMapRegistrar.h>
#include <activemq/transport/IOTransportFactory.h>
#include <activemq/exceptions/ActiveMQException.h>

namespace activemq{
namespace transport{
    
    class TcpTransportFactory : public TransportFactory
    {
    public:

    	virtual ~TcpTransportFactory(void) {}

        /**
         * Creates a Transport instance.  
         * @param properties The properties for the transport.
         * @throws ActiveMQException
         */
        virtual Transport* createTransport( 
            const activemq::util::Properties& properties )
                throw ( exceptions::ActiveMQException );

        /**
         * Returns a reference to this TransportFactory
         * @returns TransportFactory Reference
         */
        static TransportFactory& getInstance(void);

    };

}}

#endif /*_ACTIVEMQ_TRANSPORT_TCPTRANSPORTFACTORY_H_*/
