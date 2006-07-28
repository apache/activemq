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
#ifndef _ACTIVEMQ_LOGGER_FILTER_H_
#define _ACTIVEMQ_LOGGER_FILTER_H_

#include <activemq/logger/LogRecord.h>

namespace activemq{
namespace logger{
   
   /**
    * A Filter can be used to provide fine grain control over what is 
    * logged, beyond the control provided by log levels.
    * 
    * Each Logger and each Handler can have a filter associated with it. 
    * The Logger or Handler will call the isLoggable method to check if a 
    * given LogRecord should be published. If isLoggable returns false, 
    * the LogRecord will be discarded. 
    */
   class Filter
   {
   public:

      /**
       * Destructor
       */
   	virtual ~Filter(void) {}
      
      /**
       * Check if a given log record should be published.
       * @param the <code>LogRecord</code> to check.
       */
      virtual bool isLoggable(const LogRecord& record) const = 0;

   };

}}

#endif /*_ACTIVEMQ_LOGGER_FILTER_H_*/
