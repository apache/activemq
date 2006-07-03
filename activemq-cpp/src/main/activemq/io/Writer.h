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
#ifndef ACTIVEMQ_IO_WRITER_H
#define ACTIVEMQ_IO_WRITER_H

#include <string>
#include <activemq/io/IOException.h>
#include <activemq/io/OutputStream.h>

namespace activemq{
namespace io{

   /*
    * Writer interface for an object that wraps around an output
    * stream 
    */
   class Writer
   {
   public:
   
      virtual ~Writer(){};
       
      /**
       * Sets the target output stream.
       */
      virtual void setOutputStream( OutputStream* os ) = 0;
       
      /**
       * Gets the target output stream.
       */
      virtual OutputStream* getOutputStream() = 0;
             
      /**
       * Writes a byte array to the output stream.
       * @param buffer a byte array
       * @param count the number of bytes in the array to write.
       * @throws IOException thrown if an error occurs.
       */
      virtual void write(const unsigned char* buffer, int count) throw(IOException) = 0;
       
       /**
        * Writes a byte to the output stream.
        * @param v The value to be written.
        * @throws IOException thrown if an error occurs.
        */
       virtual void writeByte(unsigned char v) throw(IOException) = 0;
   };

}}

#endif /*ACTIVEMQ_IO_WRITER_H*/
