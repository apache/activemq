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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDLISTENER_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDLISTENER_H_

#include <activemq/connector/stomp/commands/StompCommand.h>
#include <activemq/connector/stomp/StompConnectorException.h>

namespace activemq{
namespace connector{
namespace stomp{

    /**
     * Interface class for object that with to register with the Stomp
     * Connector in order to process a Command that was received.
     */
    class StompCommandListener
    {
    public:
    
        virtual ~StompCommandListener(void) {}
    
        /**
         * Process the Stomp Command
         * @param command to process
         * @throw ConnterException
         */
        virtual void onStompCommand( commands::StompCommand* command ) 
            throw ( StompConnectorException ) = 0;

    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDLISTENER_H_*/
