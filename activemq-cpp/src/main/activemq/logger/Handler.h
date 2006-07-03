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
#ifndef _ACTIVEMQ_LOGGER_HANDLER_H_
#define _ACTIVEMQ_LOGGER_HANDLER_H_

#include <cms/Closeable.h>
#include <activemq/logger/LogRecord.h>

namespace activemq{
namespace logger{

   class Filter;
   class Formatter;
   
   /**
    * A Handler object takes log messages from a Logger and exports them. 
    * It might for example, write them to a console or write them to a file, 
    * or send them to a network logging service, or forward them to an OS 
    * log, or whatever.
    * 
    * A Handler can be disabled by doing a setLevel(Level.OFF) and can be 
    * re-enabled by doing a setLevel with an appropriate level.
    * 
    * Handler classes typically use LogManager properties to set default 
    * values for the Handler's Filter, Formatter, and Level. See the 
    * specific documentation for each concrete Handler class. 
    */
   class Handler : public cms::Closeable
   {
   public:
   
      /**
       * Destructor
       */
      virtual ~Handler(void) {}
      
      /**
       * Flush the Handler's output, clears any buffers.
       */
      virtual void flush(void) = 0;
      
      /**
       * Publish the Log Record to this Handler
       * @param The Log Record to Publish
       */
      virtual void publish(const LogRecord& record) = 0;
      
      /**
       * Check if this Handler would actually log a given LogRecord.
       * <p>
       * This method checks if the LogRecord has an appropriate Level and 
       * whether it satisfies any Filter. It also may make other Handler 
       * specific checks that might prevent a handler from logging the 
       * LogRecord.
       * @param <code>LogRecord</code> to check
       */
      virtual void isLoggable(const LogRecord& record) = 0;
      
      /**
       * Sets the Filter that this Handler uses to filter Log Records
       * <p>
       * For each call of publish the Handler will call this Filter (if it 
       * is non-null) to check if the LogRecord should be published or 
       * discarded.
       * @param <code>Filter</code> derived instance
       */
      virtual void setFilter(const Filter* filter) = 0;
      
      /**
       * Gets the Filter that this Handler uses to filter Log Records
       * @param <code>Filter</code> derived instance
       */
      virtual const Filter* getFilter(void) = 0;
      
      /**
       * Set the log level specifying which message levels will be logged 
       * by this Handler.
       * <p>
       * The intention is to allow developers to turn on voluminous logging, 
       * but to limit the messages that are sent to certain Handlers.
       * @param Level enumeration value
       */
      virtual void setLevel(Level value) = 0;
      
      /**
       * Get the log level specifying which message levels will be logged 
       * by this Handler.
       * @param Level enumeration value
       */
      virtual Level getLevel(void) = 0;
      
      /**
       * Sets the <code>Formatter</code> used by this Handler
       * <p>
       * Some Handlers may not use Formatters, in which case the 
       * Formatter will be remembered, but not used.
       * @param <code>Filter</code> derived instance
       */
      virtual void setFormatter(const Formatter* formatter) = 0;
      
      /**
       * Gets the <code>Formatter</code> used by this Handler
       * @param <code>Filter</code> derived instance
       */
      virtual const Formatter* getFormatter(void) = 0;

   };

}}

#endif /*_ACTIVEMQ_LOGGER_HANDLER_H_*/
