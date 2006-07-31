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

#ifndef ACTIVEMQ_NONBLOCKINGMSGCONSUMER_REF_H
#define ACTIVEMQ_NONBLOCKINGMSGCONSUMER_REF_H

#include "MessageConsumerRef.h"

namespace ActiveMQ {
    class CoreLib;
    class NonBlockingMessageConsumer;

    /// handle on message vendor with nonblocking semantics.
    /**
       This class is a handle on a NonBlockingMessageConsumer, which
       is an object that holds messages that have been received by a
       BrokerSession object but not delivered to the application.  It
       contains an internal queue and an event file descriptor.

       When a Message is enqueued, it is stored internally and the
       event file descriptor will become read active.  When there are
       no messages ready, it will be read unactive.  <b>Do not read
       data from this file descriptor</b> - no more than one byte is
       ever actually on it.

       The receive() call will return NULL if no message is ready.  If
       a message is ready, it will return the message.

       NonBlockingMessageConsumers are owned by the library, so this
       weak reference class is provided to allow flexible sharing of
       the handle (like a pointer) while at the same time providing
       protection from pointer invalidation.  When the owning library
       is destructed, it will nullify existing weak reference objects
       so that calls will throw instead of crashing.

       @version $Id$
    */
    class NonBlockingMessageConsumerRef : public MessageConsumerRef {
    public:
        NonBlockingMessageConsumerRef();
        NonBlockingMessageConsumerRef(const NonBlockingMessageConsumerRef &);
        NonBlockingMessageConsumerRef& operator=(const NonBlockingMessageConsumerRef &);
        virtual ~NonBlockingMessageConsumerRef();

        /// gets the event file descriptor
        /**
           Gets the file descriptor that will have data on it when a
           message is ready.

           @returns the event file descriptor
        */
        int getEventFD() const;

        /// gets the number of messages that are ready
        /**
           Gets the number of waiting messages.

           @returns the number
        */
        int getNumReadyMessages() const;

        /// nonblocking receive
        /**
           Receives a Message.  If none are ready, this will return
           NULL (specifically, an auto_ptr pointing to NULL).

           If the queue is emptied as a result of this call, the event
           file descriptor will become non-readable.

           Note that a variant of receive() that returns a regular
           pointer is defined on ActiveMQ::MessageConsumerRef.

           @returns the new Message
        */
        std::auto_ptr<Message> receive();

        /// checks validity
        /**
           Since this class is a weak reference, it could be
           invalidated.  If it's invalid, calling these functions will
           throw.

           @returns true if the reference is invalid
        */
        virtual bool isValid() const;

    private:
        friend class CoreLibImpl;
        NonBlockingMessageConsumerRef(CoreLib *a, NonBlockingMessageConsumer *q);
        NonBlockingMessageConsumer *cons_;
        MessageConsumer *getConsumer() const;
    };
};

#endif // ACTIVEMQ_NONBLOCKINGMSGCONSUMER_REF_H
