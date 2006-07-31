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

#ifndef BufferWriter_hpp_
#define BufferWriter_hpp_

#include <vector>

#include "marshal/BinaryWriter.h"

namespace ActiveMQ {
  namespace IO {
    class BufferWriter : public BinaryWriter
    {
      private:
        std::vector<uint8_t>& writeInto_;

      public:
        BufferWriter(std::vector<uint8_t>& writeInto) : writeInto_(writeInto) {}
        virtual ~BufferWriter() {}
      
        virtual void close();
        virtual void flush();
        virtual int write(const uint8_t* buffer, size_t size);
    };
  }
}

#endif /*BufferWriter_hpp_*/
