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
#ifndef _ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDREADER_H_
#define _ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDREADER_H_

#include <activemq/transport/CommandReader.h>
#include <activemq/io/InputStream.h>
#include <activemq/transport/CommandIOException.h>
#include <activemq/transport/Command.h>
#include <activemq/connector/stomp/StompFrame.h>
#include <activemq/connector/stomp/StompConnectorException.h>
#include <activemq/connector/stomp/marshal/Marshaler.h>

namespace activemq{
namespace connector{
namespace stomp{

    class StompCommandReader : public transport::CommandReader
    {
    private:
   
        /**
         * The target input stream.
         */
        io::InputStream* inputStream;
      
        /**
         * Vector Object used to buffer data
         */
        std::vector<unsigned char> buffer;
        
        /**
         * Marshaler of Stomp Commands
         */
        marshal::Marshaler marshaler;

    public:

        /**
         * Deafult Constructor
         */
        StompCommandReader( void );

        /**
         * Constructor.
         * @param is the target input stream.
         */
        StompCommandReader( io::InputStream* is );

        virtual ~StompCommandReader(void) {}

        /**
         * Reads a command from the given input stream.
         * @return The next command available on the stream.
         * @throws CommandIOException if a problem occurs during the read.
         */
        virtual transport::Command* readCommand(void) 
            throw ( transport::CommandIOException );

        /**
         * Sets the target input stream.
         * @param Target Input Stream
         */
        virtual void setInputStream( io::InputStream* is ){
            inputStream = is;
        }
      
        /**
         * Gets the target input stream.
         * @return Target Input Stream
         */
        virtual io::InputStream* getInputStream(void){
            return inputStream;
        }

        /**
         * Attempts to read an array of bytes from the stream.
         * @param buffer The target byte buffer.
         * @param count The number of bytes to read.
         * @return The number of bytes read.
         * @throws IOException thrown if an error occurs.
         */
        virtual int read( unsigned char* buffer, int count ) 
            throw( io::IOException );
       
        /**
         * Attempts to read a byte from the input stream
         * @return The byte.
         * @throws IOException thrown if an error occurs.
         */
        virtual unsigned char readByte(void) throw( io::IOException );

    private:
    
        /**
         * Read the Stomp Command from the Frame
         * @param reference to a Stomp Frame
         * @throws StompConnectorException
         */
        void readStompCommand( StompFrame& frame ) 
            throw ( StompConnectorException );

        /** 
         * Read all the Stomp Headers for the incoming Frame
         * @param Frame to place data into
         * @throws StompConnectorException
         */
        void readStompHeaders( StompFrame& frame ) 
            throw ( StompConnectorException );

        /**
         * Reads a Stomp Header line and stores it in the buffer object
         * @return number of bytes read, zero if there was a problem.
         * @throws StompConnectorException
         */
        int readStompHeaderLine(void) throw ( StompConnectorException );

        /**
         * Reads the Stomp Body from the Wire and store it in the frame.
         * @param Stomp Frame to place data in
         */
        void readStompBody( StompFrame& frame ) 
            throw ( StompConnectorException );
    
    };

}}}

#endif /*_ACTIVEMQ_CONNECTOR_STOMP_STOMPCOMMANDREADER_H_*/
