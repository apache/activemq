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

#ifndef _CMS_MESSAGELISTENER_H_
#define _CMS_MESSAGELISTENER_H_

//#include <cms/Message.h>
 
namespace cms{
    
    class Message;
    
    class MessageListener{
    public:
    
        virtual ~MessageListener(void){}
        
        /**
         * Called asynchronously when a new message is received, the message
         * reference can be to any othe Message types. a dynamic cast is used
         * to find out what type of message this is.  The lifetime of this
         * object is only garunteed to be for life of the onMessage function
         * after this returns the message may no longer exists.  User should
         * copy the data or clone the message if they wish to keep something
         * around about this message.
         * 
         * It is considered a programming error for this method to throw and
         * exception.
         * 
         * @param Message object reference
         */
        virtual void onMessage( const Message& message ) = 0;

    };
    
}

#endif /*_CMS_MESSAGELISTENER_H_*/
