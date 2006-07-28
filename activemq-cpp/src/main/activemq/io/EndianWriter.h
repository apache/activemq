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
#ifndef ACTIVEMQ_IO_ENDIANWRITER_H
#define ACTIVEMQ_IO_ENDIANWRITER_H

#include <activemq/io/Writer.h>
#include <activemq/io/OutputStream.h>
#include <activemq/util/Endian.h>

namespace activemq{
namespace io{

    /*
     * The EndianWriter class writes primitive C++ data types to an
     * underlying output stream in a Java compatible way. Strings
     * are written as raw bytes, no character encoding is performed.
     *
     * All numeric data types are written in big endian (network byte
     * order) and if the platform is little endian they are converted
     * automatically.
     *
     * Should any error occur an IOException will be thrown.
     */
    class EndianWriter : public Writer
    {
    private:
    
        /**
         * Target output stream.
         */
        OutputStream* outputStream;
        
    public:
    
        /**
         * Default Constructor.
         */
        EndianWriter();
        
        /**
         * Constructor.
         * @param os the target output stream.
         */
        EndianWriter( OutputStream* os );
        
        /**
         * Destructor.
         */
        virtual ~EndianWriter();
    
        /**
         * Sets the target output stream.
         */
        virtual void setOutputStream( OutputStream* os ){
            outputStream = os;
        }
        
        /**
         * Gets the target output stream.
         */
        virtual OutputStream* getOutputStream(){
            return outputStream;
        }
        
        /**
         * Writes a byte array to the target output stream.
         * @param buffer a byte array.
         * @param count the number of bytes to write.
         * @throws IOException thrown if an error occurs.
         */
        virtual void write( const unsigned char* buffer, int count ) 
            throw( IOException );
        
        /**
         * Writes a byte to the target output stream.
         * @param v the value to be written
         * @throws IOException thrown if an error occurs.
         */
        virtual void writeByte( unsigned char v ) throw( IOException );
        
        /**
         * Writes a double to the target output stream.
         * @param v the value to be written
         * @throws IOException thrown if an error occurs.
         */
        virtual void writeDouble( double v ) throw( IOException );
        
        /**
         * Writes a float to the target output stream.
         * @param v the value to be written
         * @throws IOException thrown if an error occurs.
         */
        virtual void writeFloat( float v ) throw( IOException );
        
        /**
         * Writes a short to the target output stream.
         * @param v the value to be written
         * @throws IOException thrown if an error occurs.
         */
        virtual void writeUInt16( uint16_t v ) throw( IOException );
        
        /**
         * Writes an int to the target output stream.
         * @param v the value to be written
         * @throws IOException thrown if an error occurs.
         */
        virtual void writeUInt32( uint32_t v ) throw( IOException );
        
        /**
         * Writes a long long to the target output stream.
         * @param v the value to be written
         * @throws IOException thrown if an error occurs.
         */
        virtual void writeUInt64( uint64_t v ) throw( IOException );
    };

}}

#endif /*ACTIVEMQ_IO_ENDIANWRITER_H*/
