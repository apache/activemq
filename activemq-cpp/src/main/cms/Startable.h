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
 
#ifndef CMS_STARTABLE_H
#define CMS_STARTABLE_H
 
#include <cms/CMSException.h>

namespace cms{
    
    /**
     * Interface for a class that implements the start method.
     */
    class Startable{
        
    public:
    
        virtual ~Startable(){}
        
        /**
         * Starts the service.
         */
        virtual void start() throw( CMSException ) = 0;
        
    };
}

#endif /*CMS_STARTABLE_H*/
