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
 
#ifndef ACTIVEMQ_TRANSPORT_TRANSPORTEXCEPTIONLISTENER_H_
#define ACTIVEMQ_TRANSPORT_TRANSPORTEXCEPTIONLISTENER_H_

#include <activemq/exceptions/ActiveMQException.h>

namespace activemq{
namespace transport{
  
    // Forward declarations.
    class Transport;
  
    /**
     * A listener of asynchronous exceptions from a command transport object.
     */
    class TransportExceptionListener{
    public:
        
        virtual ~TransportExceptionListener(){}
        
        /**
         * Event handler for an exception from a command transport.
         * @param source The source of the exception
         * @param ex The exception.
         */
        virtual void onTransportException( 
            Transport* source, 
            const exceptions::ActiveMQException& ex ) = 0;
            
    };
    
}}

#endif /*ACTIVEMQ_TRANSPORT_TRANSPORTEXCEPTIONLISTENER_H_*/
