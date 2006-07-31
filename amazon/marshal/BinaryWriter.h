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
#ifndef BinaryWriter_hpp_
#define BinaryWriter_hpp_

#include "marshal/IWriter.h"

namespace ActiveMQ {
  namespace IO {

    /*
     * The BinaryWriter class writes primitive C++ data types to an
     * underlying output stream in a Java compatible way. Strings
     * are written as raw bytes, no character encoding is performed.
     *
     * All numeric data types are written in big endian (network byte
     * order) and if the platform is little endian they are converted
     * automatically.
     *
     * Should any error occur an IOException will be thrown.
     */
    class BinaryWriter : public IWriter
    {
      public:
        virtual ~BinaryWriter() {}

        virtual void close() = 0;
        virtual void flush() = 0;
        virtual int write(const uint8_t* buffer, size_t size) = 0;
        virtual void writeByte(uint8_t v);
        virtual void writeBoolean(bool v);
        virtual void writeDouble(double v);
        virtual void writeFloat(float v);
        virtual void writeShort(short v);
        virtual void writeInt(int v);
        virtual void writeLong(int64_t v);
        virtual void writeString(const std::string& v);
    };
  }
}

#endif /*BinaryWriter_hpp_*/
