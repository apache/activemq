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
#ifndef IReader_hpp_
#define IReader_hpp_

#include <inttypes.h>

#include <string>
#include <vector>

namespace ActiveMQ {
  namespace IO {

    /*
     * The IReader interface provides for reading bytes from a binary stream
     * and reconstructing from them data in any of the C++ primitive types.
     * Strings are read as raw bytes, no character decoding is performed.
     */
    class IReader
    {
      public:
        virtual ~IReader() {};

        virtual void close() = 0;
        virtual int read(uint8_t* buffer, size_t size) = 0;
        virtual unsigned char readByte() = 0;
        virtual bool readBoolean() = 0;
        virtual double readDouble() = 0;
        virtual float readFloat() = 0;
        virtual short readShort() = 0;
        virtual int readInt() = 0;
        virtual int64_t readLong() = 0;
        virtual std::string readString() = 0;
    };
  }
}

#endif /*IReader_hpp_*/
