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
#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDWRITER_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDWRITER_H_

#include <activemq/transport/CommandWriter.h>
#include <activemq/io/InputStream.h>
#include <activemq/transport/CommandIOException.h>
#include <activemq/connector/stomp/StompConnectorException.h>
#include <activemq/transport/Command.h>
#include <activemq/io/OutputStream.h>
#include <activemq/connector/stomp/marshal/Marshaler.h>

namespace activemq{
namespace connector{
namespace stomp{

    class StompCommandWriter : public transport::CommandWriter
    {
    private:
    
        /**
         * Target output stream.
         */
        io::OutputStream* outputStream;

        /**
         * Marshaler of Stomp Commands
         */
        marshal::Marshaler marshaler;

    public:
    
        /**
         * Default Constructor
         */
    	StompCommandWriter(void);

        /**
         * Constructor.
         * @param os the target output stream.
         */
        StompCommandWriter( io::OutputStream* os );

        /**
         * Destructor
         */
    	virtual ~StompCommandWriter(void) {}

        /**
         * Sets the target output stream.
         */
        virtual void setOutputStream(io::OutputStream* os){
            outputStream = os;
        }
      
        /**
         * Gets the target output stream.
         */
        virtual io::OutputStream* getOutputStream(void){
            return outputStream;
        }

        /**
         * Writes a command to the given output stream.
         * @param command the command to write.
         * @param os the target stream for the write.
         * @throws CommandIOException if a problem occurs during the write.
         */
        virtual void writeCommand( const transport::Command* command ) 
            throw ( transport::CommandIOException );

        /**
         * Writes a byte array to the output stream.
         * @param buffer a byte array
         * @param count the number of bytes in the array to write.
         * @throws IOException thrown if an error occurs.
         */
        virtual void write(const unsigned char* buffer, int count) 
            throw( io::IOException );
       
        /**
         * Writes a byte to the output stream.
         * @param v The value to be written.
         * @throws IOException thrown if an error occurs.
         */
        virtual void writeByte(unsigned char v) throw( io::IOException );

    private:
   
        /**
         * Writes a char array to the output stream.
         * @param buffer a char array
         * @param count the number of bytes in the array to write.
         * @throws IOException thrown if an error occurs.
         */
        virtual void write(const char* buffer, int count) 
            throw( io::IOException );

    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDWRITER_H_*/
