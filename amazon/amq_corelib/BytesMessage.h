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

#ifndef ACTIVEMQ_BYTESMESSAGE_H
#define ACTIVEMQ_BYTESMESSAGE_H

#include <vector>

#include "Message.h"

namespace ActiveMQ {
    class CoreLibImpl;

    /// Message containing a byte buffer
    /**
       Represents an ActiveMQ Message containing an opaque byte array.

       @version $Id$
    */
    class BytesMessage : public Message {
    public:
        /// gets the type
        /**
           Gets the integer type of this message.

           @returns the type
        */
        int getType() const { return Command::Types::ACTIVEMQ_BYTES_MESSAGE; }

        /// Constructs a new BytesMessage.
        /**
           Makes a new BytesMessage containing the given data.
        */
        BytesMessage(const Buffer& data) : data_(data) {}

        /// gets the data as a byte array
        /*
          @param buf the buffer to put the data into
        */
        void marshall(Buffer& buf) const { buf = data_; }
    private:
        Buffer data_;
    };
};

#endif // ACTIVEMQ_BYTESMESSAGE_H
