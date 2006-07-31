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
#ifndef BinaryReader_hpp_
#define BinaryReader_hpp_

#include <string>
#include <vector>
#include "IReader.h"

namespace ActiveMQ {
  namespace IO {
        
    /*
     * The BinaryReader class reads primitive C++ data types from an
     * underlying input stream in a Java compatible way. Strings are
     * read as raw bytes, no character decoding is performed.
     *
     * All numeric data types are assumed to be available in big
     * endian (network byte order) and are converted automatically
     * to little endian if needed by the platform.
     */
    class BinaryReader : public IReader
    {
      public:
        virtual ~BinaryReader() {}

        virtual void close() = 0;
        virtual int read(uint8_t* buffer, size_t size) = 0;
        virtual uint8_t readByte();
        virtual bool readBoolean();
        virtual double readDouble();
        virtual float readFloat();
        virtual short readShort();
        virtual int readInt();
        virtual int64_t readLong();
        virtual std::string readString();
    };
  }
}

#endif /*BinaryReader_hpp_*/
