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

#ifndef ACTIVEMQ_BROKERSESSION_H
#define ACTIVEMQ_BROKERSESSION_H

#include <string>
#include <memory>

#include "BrokerSessionImpl_.h"

#include "amq_corelib/ExceptionCallback.h"
#include "amq_corelib/Logger.h"

/// Holds all of the ActiveMQ-related classes
/**
   This namespace contains all classes needed to do asynchronous
   messaging with ActiveMQ.
*/
namespace ActiveMQ {
    class NonBlockingMessageConsumerRef;
    class BlockingMessageConsumerRef;
    class MessageConsumerRef;
    class Transport;
    class Destination;
    class Message;

    /// High-level ActiveMQ messaging library with background thread
    /**
       This class is the primary class for use with ActiveMQ if your application:
         - doesn't have out-of-the-ordinary needs for message transport
         - can tolerate a background thread
       
       This library creates a background thread which listens to
       messages from ActiveMQ.  This thread takes care of shuffling
       data between the core library(see ActiveMQ::CoreLib) and the
       transport represented by the URI provided.

       The BrokerSession library is able to be shared between multiple
       threads (see ActiveMQ::BlockingMessageConsumer for a
       multi-thread-friendly way to have messages delivered).

       @version $Id$
    */
    class BrokerSession {
    public:
        /// Creates a new handle on an ActiveMQ broker.
        /**
           Creates a new (unconnected) handle and state associated
           with a logical ActiveMQ broker.

           @param uri uri of the broker
           @param user username to use for authentication
           @param password password to use for authentication
        */
        BrokerSession(const std::string& uri,
                      const std::string& user = "",
                      const std::string& password = "");

        /// Connects to the broker
        /**
           Connects to the broker and enables the other commands.

           Connection semantics / reconnects / timeouts etc work
           within URI configuration options.  See the ActiveMQ
           documentation regarding transport URIs at:

           http://www.activemq.org/Configuring+Transports
        */
        void connect();

        /// Checks connection
        /**
           Indicates whether this BrokerSession is connected.

           @returns true if connected
        */
        bool isConnected() const;

        /// Disconnects from the broker
        /**
           Disconnects from the broker and disables the other commands.
        */
        void disconnect();

        /// Publishes a message
        /**
           Publishes a message.  It extracts the Destination from the
           message and calls publish(msg,dest).

           When this method returns, the message has been guaranteed
           to have been sent to the broker.

           @param msg message to send
        */
        void publish(const Message& msg);

        /// Publishes a message
        /**
           Publishes a message on a given Destination.

           When this method returns, the message has been guaranteed
           to have been sent to the broker.

           @param dest destination of the message
           @param msg message to send
        */
        void publish(const Destination& dest, const Message& msg);

        /// Subscribes
        /**
           Subscribes this session to a particular Destination.  When
           a message is received on this Destination, it will be
           delivered to the given consumer.

           When this method returns, all new messages on the given
           Destination will be passed to the given MessageConsumer.
           Any existing MessageConsumers receiving on this Destination
           will cease to.

           @param dest destination to receive messages on
           @param q MessageConsumer to deliver messages to
        */
        void subscribe(const Destination& dest, MessageConsumerRef& q);

        /// Unsubscribes
        /**
           Stops the receipt of messages from the broker on the given
           Destination.  After this call no more messages will be
           received from the MessageConsumer subscribed to this
           Destination.

           Throws ActiveMQ::Exception if this destination is not already subscribed.
           
           @param dest the Destination to stop listening to
        */
        void unsubscribe(const Destination& dest);
        
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

        /// Sets the exception handler for internally-generated exceptions
        /**
           Since the BrokerSession library has an internal thread for
           doing I/O and interaction with the core library, it needs a
           way to communicate these exceptions to the user of the
           library.  This is done by setting this callback - any
           function or function object that can be called with a const
           ActiveMQ::Exception & will be implicitly constructed into
           an ExceptionCallback object.  The default handler simply
           prints the exception message to stderr.

           @param c The function/functor to call with an internally-generated exception.
           @returns The previously-set exception handler
        */
        ExceptionCallback setExceptionCallback(ExceptionCallback c);

        /// Gets the exception handler
        /**
           Gets the callback that will be called on exceptions.  See
           documentation for setExceptionCallback above.

           @returns the exception handler
        */
        ExceptionCallback& getExceptionCallback() const;

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

        /// Creates a new topic
        /**
           @returns a new Destination that represents a topic
        */
        Destination createTopic(const std::string& name);

        /// Creates a new queue
        /**
           @returns a new Destination that represents a queue
        */
        Destination createQueue(const std::string& name);

        /// Creates a new temporary topic
        /**
           @returns a new Destination that represents a temporary topic
        */
        Destination createTemporaryTopic();

        /// Creates a new temporary queue
        /**
           @returns a new Destination that represents a temporary queue
        */
        Destination createTemporaryQueue();

    private:
        BrokerSession(const BrokerSession &);
        BrokerSession& operator=(const BrokerSession &);

        std::auto_ptr<BrokerSessionImpl> impl_;
    };
};

#endif // ACTIVEMQ_BROKERSESSION_H
