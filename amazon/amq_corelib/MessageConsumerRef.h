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

#ifndef ACTIVEMQ_MSGCONSUMER_REF_H
#define ACTIVEMQ_MSGCONSUMER_REF_H

#include <memory>

namespace ActiveMQ {
    class Message;
    class CoreLib;
    class MessageConsumer;

    /// Base class for MessageConsumerRefs
    /**
       This class provides the common interface for
       MessageConsumerRefs, which are handles on objects allow the
       application to receive Messages.  They are weak references, so
       they can be invalid, but you are free to copy them around.  See
       the documentation for ActiveMQ::NonBlockingMessageConsumerRef
       and ActiveMQ::BlockingMessageConsumerRef.

       @version $Id$
    */
    class MessageConsumerRef {
    public:
        /// gets the number of messages that are ready
        /**
           Gets the number of waiting messages.

           @returns the number
        */
        virtual int getNumReadyMessages() const = 0;

        /// receives a message
        /**
           Pulls a message from the internal queue.  It is returned as
           a std::auto_ptr to make the ownership policy clear.

           @returns the new message
        */
        virtual std::auto_ptr<Message> receive() = 0;

        /// receives a dumb pointer to a message
        /**
           Just like receive(), but not in auto_ptr form.  <b>Note:
           the caller still owns this memory, but it's just not
           enforced by the compiler.</b> The caller is responsible for
           freeing this memory (using delete);

           @returns a dumb pointer to the message
        */
        Message *receivePtr();

        /// checks validity
        /**
           Since this class is a weak reference, it could be
           invalidated.  If it's invalid, calling any function other
           than isValid() will throw.

           @returns true if the reference is invalid
        */
        virtual bool isValid() const = 0;
        
    protected:
        MessageConsumerRef();
        MessageConsumerRef(const MessageConsumerRef &);
        MessageConsumerRef& operator=(const MessageConsumerRef &);
        virtual ~MessageConsumerRef();

        friend class CoreLibImpl;
        MessageConsumerRef(CoreLib *parent);
        CoreLib *owner_;
        virtual MessageConsumer *getConsumer() const = 0;
    private:
        void invalidate();
    };
};

#endif // ACTIVEMQ_MSGCONSUMER_REF_H
