/*
 * Copyright 2006 The Apache Software Foundation or its licensors, as
 * applicable.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef _ACTIVEMQ_CORE_ACTIVEMQMESSAGE_H_
#define _ACTIVEMQ_CORE_ACTIVEMQMESSAGE_H_

#include <cms/Message.h>

namespace activemq{
namespace core{

    class ActiveMQAckHandler;

    /**
     * Interface for all ActiveMQ Messages that will pass through the core
     * API layer.  This interface defines a method that the API uses to set
     * an Acknowledgement handler that will be called by the message when
     * a user calls the <code>acknowledge</code> method of the Message 
     * interface.  This is only done when the Session that this message
     * passes through is in Client Acknowledge mode.
     */
    class ActiveMQMessage
    {
    public:

        virtual ~ActiveMQMessage(void) {}

        /**
         * Sets the Acknowledgement Handler that this Message will use
         * when the Acknowledge method is called.
         * @param ActiveMQAckHandler
         */
        virtual void setAckHandler( ActiveMQAckHandler* handler ) = 0;
        
        /**
         * Gets the number of times this message has been redelivered.
         * @return redelivery count
         */
        virtual int getRedeliveryCount(void) const = 0;
        
        /**
         * Sets the count of the number of times this message has been 
         * redelivered
         * @param redelivery count
         */
        virtual void setRedeliveryCount( int count ) = 0;

    };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQMESSAGE_H_*/
