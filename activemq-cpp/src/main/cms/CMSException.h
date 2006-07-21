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
 
#ifndef CMS_CMSEXCEPTION_H
#define CMS_CMSEXCEPTION_H
 
// Includes
#include <string>
#include <vector>
#include <iostream>

namespace cms{
    
    /**
     * This class represents an error that has occurred in 
     * cms.
     */
    class CMSException{
        
    public:
        
        virtual ~CMSException(){}
        
        /**
         * Gets the cause of the error.
         * @return string errors message
         */
        virtual const char* getMessage() const = 0;
        
        /**
         * Provides the stack trace for every point where
         * this exception was caught, marked, and rethrown.
         * @return vector containing stack trace strings
         */
        virtual std::vector< std::pair< std::string, int> > getStackTrace() const = 0;
        
        /**
         * Prints the stack trace to std::err
         */
        virtual void printStackTrace() const = 0;
        
        /**
         * Prints the stack trace to the given output stream.
         * @param stream the target output stream.
         */
        virtual void printStackTrace( std::ostream& stream ) const = 0;
        
    };

}

#endif /*CMS_CMSEXCEPTION_H*/
