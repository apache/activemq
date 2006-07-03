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
 
#ifndef ACTIVEMQ_TRANSPORT_COMMANDWRITER_H_
#define ACTIVEMQ_TRANSPORT_COMMANDWRITER_H_

#include <activemq/io/OutputStream.h>
#include <activemq/io/Writer.h>
#include <activemq/transport/CommandIOException.h>
#include <activemq/transport/Command.h>

namespace activemq{
namespace transport{
  
    /**
     * Interface for an object responsible for writing a command
     * to an output stream.
     */
    class CommandWriter : public io::Writer
    {
    public:
  
        virtual ~CommandWriter(void) {}
        
        /**
         * Writes a command to the given output stream.
         * @param command the command to write.
         * @throws CommandIOException if a problem occurs during the write.
         */
        virtual void writeCommand( const Command* command ) 
            throw ( CommandIOException ) = 0;

    };
    
}}

#endif /*ACTIVEMQ_TRANSPORT_COMMANDWRITER_H_*/
