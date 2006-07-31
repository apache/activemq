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

#ifndef ACTIVEMQ_TEXTMESSAGE_H
#define ACTIVEMQ_TEXTMESSAGE_H

#include <string>
#include <vector>

#include "Message.h"

namespace ActiveMQ {
    class CoreLibImpl;

    /// Message containing a text string
    /**
       @version $Id$
    */       
    class TextMessage : public Message {
    public:
        /// gets the type
        /**
           Gets the integer type of this message.

           @returns the type
        */
        int getType() const { return Command::Types::ACTIVEMQ_TEXT_MESSAGE; }

        /// Constructs a new TextMessage.
        /**
           Makes a new TextMessage containing the given string.
        */
        TextMessage(const std::string& data) : text_(data) {}

        /// gets the string data
        /**
           Gets the string this TextMessage represents.

           @returns message contents
        */
        const std::string& getText() const { return text_; }

        /// gets the data as a byte array
        /**
           Gets the string data as an opaque byte array.

           @param buf the buffer the put the data in
        */
        void marshall(Buffer& buf) const;

    private:
        std::string text_;

        friend class CoreLibImpl;
        TextMessage(const Buffer& deserializeFrom);
    };
};

#endif // ACTIVEMQ_TEXTMESSAGE_H
