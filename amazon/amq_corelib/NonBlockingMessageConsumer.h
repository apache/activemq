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

#ifndef ACTIVEMQ_NONBLOCKINGMSGCONSUMER_H
#define ACTIVEMQ_NONBLOCKINGMSGCONSUMER_H

#include <deque>
#include <memory>

#include "MessageConsumer.h"

namespace ActiveMQ {
    class CoreLib;

    /// Message vendor with nonblocking semantics. (private)
    /**
       This class holds messages that have been received by a
       BrokerSession object but not delivered to the application.  It
       contains an internal queue and an event file descriptor.

       When a Message is enqueued, it is stored internally and the
       event file descriptor is made read active.  The receive() call
       will return NULL if no message is ready.  If a message is
       ready, it will return the message (the fd will remain read
       active as long as there are messages ready).

       <b>The user of the API never actually owns one of these.</b>
       The library vends instances of NonBlockingMessageConsumerRef.
       The NonBlockingMessageConsumer object is destroyed when all of
       its references have been destroyed.

       See NonBlockingMessageConsumerRef for usage details.

       <b>Unlike the BlockingMessageConsumer, this class does not do
       thread synchronization for you.</b> This is to allow use of a
       MessageConsumer without requiring that pthreads be compiled in.

       @version $Id$
    */
    class NonBlockingMessageConsumer : public MessageConsumer {
    private:
        int eventpipe_[2];
        std::deque<Message *> messages_;

        NonBlockingMessageConsumer(const NonBlockingMessageConsumer& oth);
        const NonBlockingMessageConsumer& operator=(const NonBlockingMessageConsumer& oth);

        friend class CoreLibImpl;
        NonBlockingMessageConsumer();
        virtual ~NonBlockingMessageConsumer();
        void enqueue(Message *msg);
        void removeQueued(const Destination &d);

        friend class NonBlockingMessageConsumerRef;
        int getEventFD() const { return eventpipe_[0]; }
        unsigned int getNumReadyMessages() const;
        std::auto_ptr<Message> receive();
    };
};

#endif // ACTIVEMQ_NONBLOCKINGMSGCONSUMER_H
