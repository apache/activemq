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

#ifndef ACTIVEMQ_TRANSPORT_DUMMYTRANSPORTFACTORY_H_
#define ACTIVEMQ_TRANSPORT_DUMMYTRANSPORTFACTORY_H_

#include <activemq/transport/DummyTransport.h>
#include <activemq/transport/TransportFactory.h>
#include <activemq/transport/TransportFactoryMapRegistrar.h>
#include <activemq/connector/stomp/StompResponseBuilder.h>
#include <activemq/support/LibraryInit.h>

namespace activemq{
namespace transport{
    
    /**
     * Manufactures DummyTransports, which are objects that
     * read from input streams and write to output streams.
     */
    class DummyTransportFactory : public TransportFactory{
    private:
    
 //       static TransportFactoryMapRegistrar registrar;
        
    public:
        
        virtual ~DummyTransportFactory(){}
        
        /**
         * Creates a Transport instance. 
         * @param properties The properties for the transport.
         */
        virtual Transport* createTransport( 
            const activemq::util::Properties& properties )
        {
            std::string wireFormat = 
                properties.getProperty( "wireFormat", "stomp" );

            DummyTransport::ResponseBuilder* builder = NULL;

            if( wireFormat == "stomp" )
            {
                builder = new connector::stomp::StompResponseBuilder(
                    properties.getProperty( 
                        "transport.sessionId", "testSessionId" ) );
            }
            
            return new DummyTransport( builder, true );
        }

    };
    
}}

#endif /*ACTIVEMQ_TRANSPORT_DUMMYTRANSPORTFACTORY_H_*/
