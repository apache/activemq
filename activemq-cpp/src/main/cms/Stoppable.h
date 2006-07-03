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
 
#ifndef CMS_STOPPABLE_H
#define CMS_STOPPABLE_H
 
#include <cms/CMSException.h>

namespace cms{
    
    /**
     * Interface for a class that implements the stop method.
     */
    class Stoppable{
        
    public:
    
        virtual ~Stoppable(){}
        
        /**
         * Stops this service.
         */
        virtual void stop() throw( CMSException ) = 0;
    };
}

#endif /*CMS_STOPPABLE_H*/
