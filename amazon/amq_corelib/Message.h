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

#ifndef ACTIVEMQ_MESSAGE_H
#define ACTIVEMQ_MESSAGE_H

#include <vector>

#include <time.h>

#include "Buffer.h"
#include "Destination.h"

#include "command/CommandTypes.h"

namespace ActiveMQ {
    /// Represents an ActiveMQ message
    /**
       Contains an ActiveMQ message.  The specific types
       (ObjectMessage, BytesMessage, etc) are subclasses.

       @version $Id$
    */
    class Message {
    protected:
        Message() {}
    public:
        /// gets the type
        /**
           Gets the integer type of this message.

           @returns the type
        */
        virtual int getType() const = 0;

        /// gets the destination
        /**
           Gets the Destination that this message was/will be sent to.

           @returns destination of this message
        */
        const Destination& getDestination() const { return destination_; }

        /// sets the destination
        void setDestination(const Destination& d) { destination_ = d; }

        /// gets the reply-to destination
        /**
           Gets the "reply-to" Destination for this Message.

           @returns the Destination for replies to this Message.
        */
        const Destination& getReplyTo() const { return replyTo_; }

        /// sets the reply-to destination
        /**
           Sets the "reply-to" Destination for this Message.

           @param dest the destination to ask for replies to this message on
        */
        void setReplyTo(const Destination& dest) { replyTo_ = dest; }

        /// gets the time received
        /**
           Gets the time (in seconds since the epoch) that this
           Message was received.

           @returns time message was received
        */
        time_t getTimestamp() const;

        /// gets the underlying bytes
        /**
           Gets this Message's payload as an array of bytes.

           @returns message data
        */
        virtual void marshall(Buffer& fillIn) const = 0;

        /// Destructor
        virtual ~Message() {};
    private:
        Destination destination_;
        Destination replyTo_;
    };
};

#endif // ACTIVEMQ_MESSAGE_H
