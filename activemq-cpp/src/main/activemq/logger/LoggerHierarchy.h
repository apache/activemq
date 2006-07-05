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
#ifndef _ACTIVEMQ_LOGGER_LOGGERHIERARCHY_H_
#define _ACTIVEMQ_LOGGER_LOGGERHIERARCHY_H_

namespace activemq{
namespace logger{

    class LoggerHierarchy
    {
    public:

        /**
         * Default Constructor
         */
    	LoggerHierarchy(void);

        /**
         * Destructor
         */
    	virtual ~LoggerHierarchy(void);

    };

}}

#endif /*_ACTIVEMQ_LOGGER_LOGGERHIERARCHY_H_*/
