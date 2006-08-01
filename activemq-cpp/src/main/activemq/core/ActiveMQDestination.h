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

#ifndef _ACTIVEMQ_CORE_ACTIVEMQDESTINATION_H_
#define _ACTIVEMQ_CORE_ACTIVEMQDESTINATION_H_

#include <activemq/util/SimpleProperties.h>
#include <activemq/util/StringTokenizer.h>
#include <activemq/exceptions/IllegalArgumentException.h>

#include <cms/Destination.h>

namespace activemq{
namespace core{

    template< typename T >
    class ActiveMQDestination : public T
    {
    private:

        // Params that are optional on the destination    
        util::SimpleProperties properties;

        // Destination type
        cms::Destination::DestinationType destType;

        // Name of the Destination
        std::string name;

    public:

        /**
         * Copy Consturctor
         * @param source CMS Dest to Copy, must be a compatible type
         */
        ActiveMQDestination( const cms::Destination* source ){
            this->copy( *source );
        }

        /**
         * Custom Constructor
         * @param dest string destination name plus any params
         * @param destType type of destination this represents.
         */
        ActiveMQDestination( const std::string& dest,
                             cms::Destination::DestinationType destType )
        {
            try
            {
                util::StringTokenizer tokenizer(dest, "?&");
                std::vector<std::string> tokens;

                // Set the type, we know that much anyway
                this->destType = destType;

                // Require that there at least one token, the dest
                if( tokenizer.countTokens() < 1 )
                {
                    throw exceptions::IllegalArgumentException(
                        __FILE__, __LINE__,
                        ( std::string( 
                            "ActiveMQDestination::ActiveMQDestination - "
                            "Marlformed Dest: " ) + dest ).c_str() );
                }

                // Grab the name, that's always first.
                this->name = tokenizer.nextToken();
                
                // Now get all the optional parameters and store them as properties
                int count = tokenizer.toArray( tokens );
                
                for( int i = 0; i < count; ++i )
                {
                    tokenizer.reset( tokens[i], "=" );
            
                    if( tokenizer.countTokens() != 2 )
                    {
                        throw exceptions::IllegalArgumentException(
                            __FILE__, __LINE__,
                            ( std::string( 
                                "ActiveMQDestination::ActiveMQDestination - "
                                "Marlformed Parameter = ") + tokens[i] ).c_str() );
                    }

                    // Get the out in the right order
                    std::string key   = tokenizer.nextToken();
                    std::string value = tokenizer.nextToken();

                    // Store this param as a property
                    properties.setProperty( key, value );
                }
            }
            AMQ_CATCH_RETHROW( exceptions::IllegalArgumentException )
            AMQ_CATCH_EXCEPTION_CONVERT( exceptions::ActiveMQException, exceptions::IllegalArgumentException )
            AMQ_CATCHALL_THROW( exceptions::IllegalArgumentException )            
        }
        
        virtual ~ActiveMQDestination() {}
        
        /**
         * Get the properties of this Destination, these are the optional
         * params that can be specified on a destination name i.e/
         * TEST.QUEUE?consumer.dispatchAsync=false&consumer.prefetchSize=10
         * @returns const reference to a properties object
         */
        virtual const util::Properties& getProperties() const {
            return properties;
        }
        
        /**
         * Copy the contents of the given properties object to this
         * objects Properties object.  Existing values are erased.
         * @param properties the Properties to copy to this object.
         */
        virtual void setProperties( const util::Properties& properties ){
            this->properties.copy( &properties );
        }
        
        /**
         * Gets the Destination Name minus any optional params that can
         * be appended to the destination with an ?
         * @returns destination name minus params
         */
        virtual const std::string& getName() const {
            return name;
        }
        
        /**
         * Sets the Destination Name minus any optional params that can
         * be appended to the destination with an ?
         * @param name destination name minus params
         */
        virtual void setName( const std::string& name ) {
            this->name = name;
        }
        
        /**
         * Retrieve the Destination Type for this Destination
         * @return The Destination Type
         */
        virtual cms::Destination::DestinationType getDestinationType() const {
            return destType;
        }
        
        /**
         * Set the Destination Type for this Destination
         * @param destType The Destination Type
         */
        virtual void setDestinationType( cms::Destination::DestinationType destType ) {
            this->destType = destType;
        }

        /**
         * Copies the contents of the given Destinastion object to this one.
         * @param source The source Destination object.
         */
        virtual void copy( const cms::Destination& source ) {

            try
            {            
                // This will throw an Bad Cast Exception if the destination
                // isn't a compatible type
                const ActiveMQDestination<T>& destination = 
                    dynamic_cast< const ActiveMQDestination<T>& >( source );
                
                this->name     = destination.getName();
                this->destType = destination.getDestinationType();

                this->properties.copy( &destination.getProperties() );
            }
            AMQ_CATCHALL_THROW( exceptions::ActiveMQException )
        }

    };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQDESTINATION_H_*/
