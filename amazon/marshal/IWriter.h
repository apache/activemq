/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at
  
  http://www.apache.org/licenses/LICENSE-2.0
  
  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
*/
#ifndef IWriter_hpp_
#define IWriter_hpp_

#include <sys/types.h>
#include <inttypes.h>
#include <string>

namespace ActiveMQ {
  namespace IO {

    /*
     * The IWriter interface provides for converting data from any of the
     * C++ primitive types to a series of bytes and writing these bytes to
     * a binary stream. Strings are written as raw bytes, no character
     * encoding is performed. If a byte cannot be written for any reason,
     * an IOException is thrown. 
     */
    class IWriter
    {
      public:
        virtual ~IWriter() {};  // Needed for SP's

        virtual void close() = 0;
        virtual void flush() = 0;
        virtual int write(const uint8_t* buffer, size_t size) = 0;
        virtual void writeByte(uint8_t v) = 0;
        virtual void writeBoolean(bool v) = 0;
        virtual void writeDouble(double v) = 0;
        virtual void writeFloat(float v) = 0;
        virtual void writeShort(short v) = 0;
        virtual void writeInt(int v) = 0;
        virtual void writeLong(int64_t v) = 0;
        virtual void writeString(const std::string& v) = 0;
    };
  }
}

#endif /*IWriter_hpp_*/
