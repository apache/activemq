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

#ifndef ACTIVEMQ_CONNECTOR_STOMP_MARSHALER_H_
#define ACTIVEMQ_CONNECTOR_STOMP_MARSHALER_H_

#include <activemq/transport/Command.h>
#include <activemq/connector/stomp/StompFrame.h>
#include <activemq/connector/stomp/marshal/MarshalException.h>

namespace activemq{
namespace connector{
namespace stomp{
namespace marshal{
        
    /**
     * Interface for all marshallers between Commands and
     * stomp frames.
     */
    class Marshaler
    {
    public:
   
        Marshaler(void) {}
        virtual ~Marshaler(void) {}
        
        /**
         * Marshall a Stomp Frame to a Stomp Command, the frame is now
         * owned by this Command, caller should not use the frame again.
         * @param Frame to Marshall
         * @return Newly Marshaled Stomp Message
         * @throws MarshalException
         */
        virtual transport::Command* marshal( StompFrame* frame )
            throw ( MarshalException );
      
        /**
         * Marshal a Stomp Command to a Stom Frame, if the command that
         * is past is not castable to a Stomp Command an Exception will
         * be thrown
         * @param The Stomp Command to Marshal
         * @return newly Marshaled Stomp Frame
         * @throws MarshalException
         */
        virtual const StompFrame& marshal( 
            const transport::Command* command )
                throw ( MarshalException );

    };

}}}}

#endif /*ACTIVEMQ_CONNECTOR_STOMP_MARSHALER_H_*/
