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
#ifndef _ACTIVEMQ_SUPPORT_INITDIRECTOR_H_
#define _ACTIVEMQ_SUPPORT_INITDIRECTOR_H_

namespace activemq{
namespace support{

    /*
     * Create a static instance of this class to init all static data
     * in order in this library.
     * Each package that needs initalization should create a set of
     * functions that control init and cleanup.  Each should be called
     * by this class init in the constructor and cleanup in the 
     * destructor
     */
    class InitDirector
    {
    private:
    
        static int refCount;
        
    public:

    	InitDirector(void);
    	virtual ~InitDirector(void);

    };

}}

#endif /*_ACTIVEMQ_SUPPORT_INITDIRECTOR_H_*/
