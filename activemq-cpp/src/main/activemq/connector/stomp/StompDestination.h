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

#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPDESTINATION_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPDESTINATION_H_

#include <string>

#include <activemq/core/ActiveMQDestination.h>

namespace activemq{
namespace connector{
namespace stomp{

    /**
     * Templatized Destination Class that bundles all the common aspects
     * of a Stomp Destination into one class.  The template arguement is 
     * one of Topic, Queue, TemporaryTopic, or TemporaryQueue.
     */
    template <typename T>
    class StompDestination : public core::ActiveMQDestination<T>
    {
    public:

        /**
         * Copy Consturctor
         * @param source CMS Dest to Copy, must be a compatible type
         */
    	StompDestination( const cms::Destination* source ) :
            core::ActiveMQDestination<T>( source ) {}
        
        /**
         * Custom Constructor
         * @param name string destination name plus any params
         * @param type the type of destination this represents.
         */
    	StompDestination( const std::string& name,
                          cms::Destination::DestinationType type ) :
            core::ActiveMQDestination<T>( name, type ){}

        virtual ~StompDestination(void) {}

        /**
         * Retrieves the name of this destination, plus the stomp
         * destination decorator
         * @return name in a format that is used by the broker
         */
        virtual std::string toProviderString(void) const {
            return getPrefix() + core::ActiveMQDestination<T>::getName();
        }
        
        /**
         * Converts the Destination Name into a String minus the 
         * stomp decorator
         * @return string name of the desintation
         */
        virtual std::string toString(void) const {
            return core::ActiveMQDestination<T>::getName();
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
