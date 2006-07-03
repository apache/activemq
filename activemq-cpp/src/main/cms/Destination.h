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

#ifndef _CMS_DESTINATION_H_
#define _CMS_DESTINATION_H_

#include <string>

namespace cms{
	
    /**
     * A Destination object encapsulates a provider-specific address. 
     */
    class Destination{
    public:
   
        enum DestinationType
        {
            TOPIC,
            QUEUE,
            TEMPORARY_TOPIC,
            TEMPORARY_QUEUE
        };
		
	public:

        /**
         * Destructor
         */	
        virtual ~Destination(void){}
      
        /**
         * Retrieve the Destination Type for this Destination
         * @return The Destination Type
         */
        virtual DestinationType getDestinationType(void) const = 0;
        
        /**
         * Converts the Destination Name into a String 
         * @return string name
         */
        virtual std::string toString(void) const = 0;

        /**
         * Converts the Destination to a String value representing the
         * Provider specific name fot this destination, which is not
         * necessarily equal to the User Supplied name of the Destination
         * @return Provider specific Name
         */
        virtual std::string toProviderString(void) const = 0;
        
        /**
         * Creates a new instance of this destination type that is a
         * copy of this one, and returns it.
         * @returns cloned copy of this object
         */
        virtual cms::Destination* clone(void) const = 0;
      
        /**
         * Copies the contents of the given Destinastion object to this one.
         * @param source The source Destination object.
         */
        virtual void copy( const cms::Destination& source ) = 0;

    };
}

#endif /*_CMS_DESTINATION_H_*/
