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

#ifndef ACTIVEMQ_MSGCONSUMER_H
#define ACTIVEMQ_MSGCONSUMER_H

#include <memory>

namespace ActiveMQ {
    class Message;
    class CoreLib;
    class Destination;

    /// Base class for MessageConsumers
    /**
       This class provides the common interface for MessageConsumers,
       which are objects that allow the application to receive
       messages.  See the documentation for
       ActiveMQ::NonBlockingMessageConsumer and
       ActiveMQ::BlockingMessageConsumer.

       Note that the application never actually deals with one of
       these directly - the application receives MessageConsumerRef
       objects - weak, counted references to MessageConsumers.

       @version $Id$
    */
    class MessageConsumer {
    public:
        /// gets the number of messages that are ready
        /**
           Gets the number of waiting messages.

           @returns the number
        */
        virtual unsigned int getNumReadyMessages() const = 0;

        /// receives a message
        /**
           Pulls a message from the internal queue.  It is returned as
           a std::auto_ptr to make the ownership policy clear.

           @returns the new message
        */
        virtual std::auto_ptr<Message> receive() = 0;

    protected:
        friend class CoreLibImpl;
        MessageConsumer() {}
        virtual ~MessageConsumer() {};
        virtual void enqueue(Message *msg) = 0;
        virtual void removeQueued(const Destination& dest) = 0;

        class HasDest {
        public:
            HasDest(const Destination& d);
            bool operator()(const Message *m);
        private:
            const Destination &dest_;
        };

    private:
        MessageConsumer(const MessageConsumer& oth);
        MessageConsumer &operator=(const MessageConsumer& oth);
    };
};

#endif // ACTIVEMQ_MSGCONSUMER_H
