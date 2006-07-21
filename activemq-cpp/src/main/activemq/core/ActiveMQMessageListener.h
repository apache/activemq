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

#ifndef _ACTIVEMQ_CORE_ACTIVEMQMESSAGELISTENER_H_
#define _ACTIVEMQ_CORE_ACTIVEMQMESSAGELISTENER_H_

#include <activemq/exceptions/ActiveMQException.h>

namespace activemq{
namespace core{

    class ActiveMQMessage;

    class ActiveMQMessageListener
    {
    public:

        virtual ~ActiveMQMessageListener(void) {}

        /**
         * Called asynchronously when a new message is received, the message
         * that is passed is now the property of the callee, and the caller
         * will disavowe all knowledge of the message, i.e Callee must delete.
         * @param Message object pointer
         */
        virtual void onActiveMQMessage( ActiveMQMessage* message ) 
            throw ( exceptions::ActiveMQException ) = 0;

    };

}}

#endif /*_ACTIVEMQ_CORE_ACTIVEMQMESSAGELISTENER_H_*/
