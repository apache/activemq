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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPTOPIC_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPTOPIC_H_

#include <activemq/connector/stomp/StompDestination.h>
#include <activemq/connector/stomp/commands/CommandConstants.h>
#include <cms/Topic.h>

namespace activemq{
namespace connector{
namespace stomp{
    
    class StompTopic : public StompDestination<cms::Topic>
    {
    public:

        StompTopic(void) : StompDestination<cms::Topic>() {}

        StompTopic(const std::string& name) : 
            StompDestination< cms::Topic >( name, cms::Destination::TOPIC )
        {}

        virtual ~StompTopic(void) {}

        /**
         * Gets the name of this queue.
         * @return The queue name.
         */
        virtual std::string getTopicName(void) const 
            throw( cms::CMSException ) {
                return toString();
        }

        /**
         * Creates a new instance of this destination type that is a
         * copy of this one, and returns it.
         * @returns cloned copy of this object
         */
        virtual cms::Destination* clone(void) const {
            return new StompTopic( toString() );
        }

    protected:

        /**
         * Retrieves the proper Stomp Prefix for the specified type
         * of Destination
         * @return string prefix
         */
        virtual std::string getPrefix(void) const {
            return commands::CommandConstants::topicPrefix;
        }

    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPTOPIC_H_*/
