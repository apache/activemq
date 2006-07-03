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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPDESTINATION_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPDESTINATION_H_

#include <string>

#include <cms/Destination.h>

namespace activemq{
namespace connector{
namespace stomp{

    /**
     * Templatized Destination Class that bundles all the common aspects
     * of a Stomp Destination into one class.  The template arguement is 
     * one of Topic, Queue, TemporaryTopic, or TemporaryQueue.
     */
    template <typename T>
    class StompDestination : public T
    {
    private:
    
        // Destination type
        cms::Destination::DestinationType destType;
        
        // Name of the Destination
        std::string name;
        
    public:

    	StompDestination(void) {}
        
    	StompDestination( const std::string& name,
                          cms::Destination::DestinationType type )
        {
            this->name = name;
            this->destType = type;
        }

        virtual ~StompDestination(void) {}

        /**
         * Retrieves the name of this destination, plus the stomp
         * destination decorator
         * @return name
         */
        virtual std::string toProviderString(void) const {
            return getPrefix() + name;
        }
        
        /**
         * Retrieve the Destination Type for this Destination
         * @return The Destination Type
         */
        virtual cms::Destination::DestinationType getDestinationType(void) const {
            return destType;
        }
        
        /**
         * Converts the Destination Name into a String minus the 
         * stomp decorator
         * @return string name
         */
        virtual std::string toString(void) const {
            return name;
        }

        /**
         * Copies the contents of the given Destinastion object to this one.
         * @param source The source Destination object.
         */
        virtual void copy( const cms::Destination& source ) {
            this->destType = source.getDestinationType();
            this->name = source.toString();
        }

    protected:
    
        /**
         * Retrieves the proper Stomp Prefix for the specified type
         * of Destination
         * @return string prefix
         */
        virtual std::string getPrefix(void) const = 0;

    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPDESTINATION_H_*/
