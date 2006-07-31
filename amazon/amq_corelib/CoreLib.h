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

#ifndef ACTIVEMQ_CORELIB_H
#define ACTIVEMQ_CORELIB_H

#include <string>
#include <memory>

#include "Buffer.h"
#include "Logger.h"
#include "CoreLibImpl_.h"

namespace ActiveMQ {
    class MessageConsumer;
    class MessageConsumerRef;
    class NonBlockingMessageConsumerRef;
    class BlockingMessageConsumerRef;
    class Destination;
    class Message;
    /// Low-level, single-threaded library for ActiveMQ messaging
    /**
       This class is a bare-bones container for ActiveMQ messaging
       functionality.  Use this library if your application:
         - has a restricted number of threads
         - needs a simple interface
         - has nonstandard transport needs

       This library works on a "data in, messages out" model.  The
       calling application is responsible for doing socket
       communication (likely using a subclass of ActiveMQ::Transport)
       and shuttling data in and out of this library.  This is to make
       the core library as small and portable as possible, as well as
       guaranteeing that it will never block.  Most API functions
       either take a buffer that has been received or return a
       marshalled buffer with outgoing data.

       @version $Id$
    */
    class CoreLib {
    public:
        /// Creates a new handle on an ActiveMQ broker.
        /**
           Creates a new handle and state associated
           with a logical ActiveMQ broker.

           @param user username to use for authentication
           @param password password to use for authentication
        */
        CoreLib(const std::string& user,
                const std::string& password);
        
        /// Initiates OpenWire communication
        /**
           Gets initialization data and enables the other commands.

           @param toSend buffer to fill in with data to send to the broker
        */
        void initialize(Buffer& toSend);

        /// Disconnects from the broker
        /**
           Disconnects from the broker and disables the other commands.

           @param toSend buffer to fill in with data to send to the broker
        */
        void disconnect(Buffer& toSend);

        /// Publishes a message
        /**
           Publishes a message.  It extracts the Destination from the
           message and calls publish(msg,dest).

           @param msg message to send
           @param toSend buffer to fill in with data to send to the broker
        */
        void publish(const Message& msg, Buffer& toSend);

        /// Publishes a message
        /**
           Publishes a message on a given Destination.  This overrides
           any destination set as part of the Message.

           @param dest destination of the message
           @param msg message to send
           @param toSend buffer to fill in with data to send to the broker
        */
        void publish(const Destination& dest,
                     const Message& msg,
                     Buffer& toSend);

        /// Subscribes
        /**
           Subscribes this session to a particular Destination.  When
           data is received that forms a Message on this Destination,
           it will be enqueued on the given MessageConsumer.

           @param dest destination to receive messages on
           @param q MessageConsumer to deliver message to
           @param toSend buffer to fill in with data to send to the broker
        */
        void subscribe(const Destination& dest,
                       MessageConsumerRef& q,
                       Buffer& toSend);

        /// Unsubscribes
        /**
           Stops the receipt of further messages on the given
           Destination.  After this call no more messages will be
           received from the MessageConsumer subscribed to this
           Destination.

           Throws ActiveMQ::Exception if this destination is not already subscribed.
           
           @param dest the Destination to stop listening to
           @param toSend buffer to fill in with data to send to the broker
        */
        void unsubscribe(const Destination& dest,
                         Buffer& toSend);

        /// Notifies the library of data
        /**
           When there is data from the broker, pass it to this
           function.  If the incoming data forms a message, it will be
           delivered to the appropriate MessageConsumer.

           @param incoming the data read from the broker
           @param toSend buffer to fill in with data to send to the broker
        */
        void handleData(const Buffer& incoming, Buffer& toSend);

        /// Notifies the library of data
        /**
           When there is data from the broker, pass it to this
           function.  If the incoming data forms a message, it will be
           delivered to the appropriate MessageConsumer.

           @param buf the data read from the broker
           @param len the number of bytes in buf
           @param toSend buffer to fill in with data to send to the broker
        */
        void handleData(const unsigned char *buf, size_t len, Buffer& toSend);

        /// Creates a new NonBlockingMessageConsumer
        /**
           This creates a new MessageConsumer with its own event fd
           and nonblocking semantics.
           
           @returns a weak reference to the new object (which is owned by the library).
        */
        NonBlockingMessageConsumerRef newNonBlockingMessageConsumer();

        /// Creates a new BlockingMessageConsumer
        /**
           This creates a new MessageConsumer with blocking semantics.
           
           @returns a weak reference to the new object (which is owned by the library).
        */
        BlockingMessageConsumerRef newBlockingMessageConsumer();

        /// Sets the handler for log events.
        /**
           Sets the handler for log events.  This call will deallocate
           any existing Logger set by a previous call.

           Note this is passed in as an auto_ptr to demonstrate the
           library's ownership of this memory.  If this doesn't work
           well with your application, there is a dumb pointer overload.
        
           @param lgr the subclass instance of ActiveMQ::Logger to log with
        */
        void setLogger(std::auto_ptr<Logger> lgr);

        /// Sets the handler for log events.
        /**
           Sets the handler for log events.  Any heap-allocated
           pointer to an instance of a subclass of ActiveMQ::Logger
           will be accepted.  This call will deallocate any existing
           Logger set by a previous call.
        
           <b>The library will own this pointer and take care of freeing it.</b>

           @param lgr the subclass instance of ActiveMQ::Logger to log with
        */
        void setLogger(Logger *lgr);

        /// Gets the handlers for log events
        /**
           @returns a reference to the handler for log events
        */
        Logger& getLogger();

        /// Creates a new topic
        /**
           @param name the name of the topic

           @returns a new Topic
        */
        Destination createTopic(const std::string& name);

        /// Creates a new queue
        /**
           @param name the name of the queue

           @returns a new Queue
        */
        Destination createQueue(const std::string& name);

        /// Creates a new temporary topic
        /**
           @returns a new Temporary Topic
        */
        Destination createTemporaryTopic();

        /// Creates a new temporary queue
        /**
           @returns a new Temporary Queue
        */
        Destination createTemporaryQueue();

    private:
        std::auto_ptr<CoreLibImpl> pimpl_;

        friend class BlockingMessageConsumerRef;
        friend class NonBlockingMessageConsumerRef;
        friend class MessageConsumerRef;
        void registerRef(MessageConsumerRef *mc);
        void deregisterRef(MessageConsumerRef *mc);

        friend class Destination;
        void registerDest(const Destination& d);
        void unregisterDest(const Destination& d);
    };
};

#endif // ACTIVEMQ_CORELIB_H
