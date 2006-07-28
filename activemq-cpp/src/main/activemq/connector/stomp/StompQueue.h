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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPQUEUE_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPQUEUE_H_

#include <activemq/connector/stomp/StompDestination.h>
#include <activemq/connector/stomp/commands/CommandConstants.h>
#include <cms/Queue.h>

namespace activemq{
namespace connector{
namespace stomp{

    class StompQueue : public StompDestination< cms::Queue >
    {
    public:

        /**
         * Copy Consturctor
         * @param CMS Dest to Copy, must be a compatible type
         */
    	StompQueue( const cms::Destination* source ) : 
            StompDestination< cms::Queue >( source ) {}

        /**
         * Custom Constructor
         * @param string destination name plus any params
         * @param type of destination this represents.
         */
        StompQueue(const std::string& name) : 
            StompDestination< cms::Queue >( name, cms::Destination::QUEUE )
        {}

        virtual ~StompQueue(void) {}

        /**
         * Gets the name of this queue.
         * @return The queue name.
         */
        virtual std::string getQueueName(void) const 
            throw( cms::CMSException ) {
                return toString();
        }

        /**
         * Creates a new instance of this destination type that is a
         * copy of this one, and returns it.
         * @returns cloned copy of this object
         */
        virtual cms::Destination* clone(void) const {
            return new StompQueue( toString() );
        }

    protected:

        /**
         * Retrieves the proper Stomp Prefix for the specified type
         * of Destination
         * @return string prefix
         */
        virtual std::string getPrefix(void) const {
            return commands::CommandConstants::queuePrefix;
        }
        
    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPQUEUE_H_*/
