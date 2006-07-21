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

#ifndef ACTIVEMQ_IO_OUTPUTSTREAM_H
#define ACTIVEMQ_IO_OUTPUTSTREAM_H
 
#include <cms/Closeable.h>
#include <activemq/io/IOException.h>
#include <activemq/concurrent/Synchronizable.h>

namespace activemq{
namespace io{

    /**
     * Base interface for an output stream.
     */
    class OutputStream 
    : 
        public cms::Closeable,
        public concurrent::Synchronizable
    {
    public:
    
        virtual ~OutputStream(){}
        
        /**
         * Writes a single byte to the output stream.
         * @param c the byte.
         * @throws IOException thrown if an error occurs.
         */
        virtual void write( const unsigned char c ) throw ( IOException ) = 0;
        
        /**
         * Writes an array of bytes to the output stream.
         * @param buffer The array of bytes to write.
         * @param len The number of bytes from the buffer to be written.
         * @throws IOException thrown if an error occurs.
         */
        virtual void write( const unsigned char* buffer, const int len ) 
            throw ( IOException ) = 0;
        
        /**
         * Flushes any pending writes in this output stream.
         */
        virtual void flush() throw ( IOException ) = 0;
    };
        
}}

#endif /*ACTIVEMQ_IO_OUTPUTSTREAM_H*/
