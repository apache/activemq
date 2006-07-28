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

#ifndef ACTIVEMQ_TRANSPORT_STOMP_STOMPTRANSPORTFACTORY_H_
#define ACTIVEMQ_TRANSPORT_STOMP_STOMPTRANSPORTFACTORY_H_

#include <activemq/transport/Transport.h>
#include <activemq/transport/TransportFactory.h>
#include <string>

namespace activemq{
namespace transport{
namespace stomp{
    
    /**
     * Manufactures transports for a particular protocol.
     * @author Nathan Mittler
     */
    class StompTransportFactory : public TransportFactory{
    public:
    
        virtual ~StompTransportFactory(){}
        
        /**
         * Manufactures a transport object with a default login.
         * @param brokerUrl The URL of the broker.
         */
        virtual Transport* createTransport( const char* brokerUrl );
        
        /**
         * Manufactures a transport object.
         * @param brokerUrl The URL of the broker
         * @param userName The login for the broker.
         * @param password The password for the broker login.
         */
        virtual Transport* createTransport( const char* brokerUrl, 
            const char* userName,
            const char* password );
            
    private:
    
        void parseUrl( const char* brokerUrl );        
        
    private:
    
        std::string brokerHost;
        int brokerPort;
        std::string userName;
        std::string password;
    };
}}}

#endif /*ACTIVEMQ_TRANSPORT_STOMP_STOMPTRANSPORTFACTORY_H_*/
