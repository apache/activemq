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
#ifndef _ACTIVEMQ_LOGGER_CONSOLEHANDLER_H_
#define _ACTIVEMQ_LOGGER_CONSOLEHANDLER_H_

#include <activemq/logger/StreamHandler.h>
#include <activemq/io/StandardErrorOutputStream.h>

namespace activemq{
namespace logger{

    /**
     * This Handler publishes log records to System.err. By default the
     * SimpleFormatter is used to generate brief summaries.
     * 
     * Configuration: By default each ConsoleHandler is initialized using 
     * the following LogManager configuration properties. If properties are
     * not defined (or have invalid values) then the specified default 
     * values are used.
     *
     * ConsoleHandler.level specifies the default level for the Handler 
     *  (defaults to Level.INFO).
     * ConsoleHandler.filter specifies the name of a Filter class to use 
     *  (defaults to no Filter).
     * ConsoleHandler.formatter specifies the name of a Formatter class to 
     *  use (defaults to SimpleFormatter).
     */
    class ConsoleHandler
    {
    private:
   
        // The Standard Error Stream to log to
        io::StandardErrorOutputStream stream;
      
        // The default Simple Formatter
        SimpleFormatter formatter;
   
    public:
   
        ConsoleHandler(void) : StreamHandler(&stream, &formatter)
        {
            // Defaults level to Info
            setLevel(Level.INFO);
        }
      
        virtual ~ConsoleHandler(void) {}
      
        /**
         * Close the current output stream.
         * <p>
         * Override the StreamHandler close to flush the Std Err stream
         * but doesn't close.
         * @throw CMSException
         */
        virtual void close(void) throw ( cms::CMSException )
        {
            if(getOutputStream())
            {
                getOutputStream->flush();
            }
        }      

    };

}}

#endif /*_ACTIVEMQ_LOGGER_CONSOLEHANDLER_H_*/
